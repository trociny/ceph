// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageSync.h"
#include "InstanceWatcher.h"
#include "ProgressContext.h"
#include "common/errno.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSync: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {

using namespace image_sync;
using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
class ImageSync<I>::ImageCopyProgressContext : public librbd::ProgressContext {
public:
  ImageCopyProgressContext(ImageSync *image_sync) : image_sync(image_sync) {
  }

  int update_progress(uint64_t offset, uint64_t size) override {
    int percent = 100 * offset / size;
    image_sync->update_progress("COPY_IMAGE " + stringify(percent) + "%");
    return 0;
  }

  ImageSync *image_sync;
};

template <typename I>
ImageSync<I>::ImageSync(I *local_image_ctx, I *remote_image_ctx,
                        const std::string &mirror_uuid, Journaler *journaler,
                        MirrorPeerClientMeta *client_meta,
                        ContextWQ *work_queue,
                        InstanceWatcher<I> *instance_watcher,
                        Context *on_finish, ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::ImageSync", local_image_ctx->cct, on_finish),
    m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_mirror_uuid(mirror_uuid), m_journaler(journaler),
    m_client_meta(client_meta), m_work_queue(work_queue),
    m_instance_watcher(instance_watcher), m_progress_ctx(progress_ctx),
    m_lock(unique_lock_name("ImageSync::m_lock", this)) {
}

template <typename I>
ImageSync<I>::~ImageSync() {
  assert(m_image_copy_request == nullptr);
  assert(m_image_copy_prog_ctx == nullptr);
}

template <typename I>
void ImageSync<I>::send() {
  send_notify_sync_request();
}

template <typename I>
void ImageSync<I>::cancel() {
  Mutex::Locker locker(m_lock);

  dout(20) << dendl;

  m_canceled = true;

  if (m_instance_watcher->cancel_sync_request(m_local_image_ctx->id)) {
    return;
  }

  if (m_image_copy_request != nullptr) {
    m_image_copy_request->cancel();
  }
}

template <typename I>
void ImageSync<I>::send_notify_sync_request() {
  update_progress("NOTIFY_SYNC_REQUEST");

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_notify_sync_request>(this);
  m_instance_watcher->notify_sync_request(m_local_image_ctx->id, ctx);
}

template <typename I>
void ImageSync<I>::handle_notify_sync_request(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    BaseRequest::finish(r);
    return;
  }

  send_prune_catch_up_sync_point();
}

template <typename I>
void ImageSync<I>::send_prune_catch_up_sync_point() {
  update_progress("PRUNE_CATCH_UP_SYNC_POINT");

  if (m_client_meta->sync_points.empty()) {
    send_create_sync_point();
    return;
  }

  dout(20) << dendl;

  // prune will remove sync points with missing snapshots and
  // ensure we have a maximum of one sync point (in case we
  // restarted)
  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_catch_up_sync_point>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, false, m_journaler, m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_catch_up_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune catch-up sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_sync_point();
}

template <typename I>
void ImageSync<I>::send_create_sync_point() {
  update_progress("CREATE_SYNC_POINT");

  // TODO: when support for disconnecting laggy clients is added,
  //       re-connect and create catch-up sync point
  if (m_client_meta->sync_points.size() > 0) {
    send_set_sync_point_snap_context();
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_create_sync_point>(this);
  SyncPointCreateRequest<I> *request = SyncPointCreateRequest<I>::create(
    m_remote_image_ctx, m_mirror_uuid, m_journaler, m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_create_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create sync point: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_set_sync_point_snap_context();
}

template <typename I>
void ImageSync<I>::send_set_sync_point_snap_context() {
  m_lock.Lock();
  if (m_canceled) {
    m_lock.Unlock();
    finish(-ECANCELED);
    return;
  }
  m_lock.Unlock();

  dout(20) << dendl;

  assert(!m_client_meta->sync_points.empty());
  auto &sync_point = m_client_meta->sync_points.front();
  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_set_sync_point_snap_context>(this);

  update_progress("SET_SYNC_POINT_SNAP_CONTEXT");

  m_remote_image_ctx->state->snap_set(cls::rbd::UserSnapshotNamespace(),
                                      sync_point.snap_name.c_str(), ctx);
}

template <typename I>
void ImageSync<I>::handle_set_sync_point_snap_context(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to set snap context: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_copy_image();
}

template <typename I>
void ImageSync<I>::send_copy_image() {
  m_lock.Lock();
  if (m_canceled) {
    m_lock.Unlock();
    finish(-ECANCELED);
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_copy_image>(this);
  m_image_copy_prog_ctx = new ImageCopyProgressContext(this);
  m_image_copy_request = librbd::DeepCopyRequest<I>::create(
    m_remote_image_ctx, m_local_image_ctx, m_work_queue,
    m_image_copy_prog_ctx, ctx);
  m_image_copy_request->get();
  m_lock.Unlock();

  update_progress("COPY_IMAGE");

  m_image_copy_request->send();
}

template <typename I>
void ImageSync<I>::handle_copy_image(int r) {
  dout(20) << ": r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_image_copy_request->put();
    m_image_copy_request = nullptr;
    delete m_image_copy_prog_ctx;
    m_image_copy_prog_ctx = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
    }
  }

  if (r == -ECANCELED) {
    dout(10) << ": image copy canceled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to copy image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_unset_sync_point_snap_context();
}

template <typename I>
void ImageSync<I>::send_unset_sync_point_snap_context() {
  m_lock.Lock();
  if (m_canceled) {
    m_lock.Unlock();
    finish(-ECANCELED);
    return;
  }
  m_lock.Unlock();

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_unset_sync_point_snap_context>(this);

  update_progress("UNSET_SYNC_POINT_SNAP_CONTEXT");

  m_remote_image_ctx->state->snap_set(cls::rbd::UserSnapshotNamespace(), "",
                                      ctx);
}

template <typename I>
void ImageSync<I>::handle_unset_sync_point_snap_context(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to set snap context: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_prune_sync_points();
}

template <typename I>
void ImageSync<I>::send_prune_sync_points() {
  dout(20) << dendl;

  update_progress("PRUNE_SYNC_POINTS");

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_sync_points>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, true, m_journaler, m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_sync_points(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_client_meta->sync_points.empty()) {
    send_copy_image();
    return;
  }

  finish(0);
}

template <typename I>
void ImageSync<I>::update_progress(const std::string &description) {
  dout(20) << ": " << description << dendl;

  if (m_progress_ctx) {
    m_progress_ctx->update_progress("IMAGE_SYNC/" + description);
  }
}

template <typename I>
void ImageSync<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_instance_watcher->notify_sync_complete(m_local_image_ctx->id);
  BaseRequest::finish(r);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSync<librbd::ImageCtx>;
