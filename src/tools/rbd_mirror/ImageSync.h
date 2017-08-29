// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_H
#define RBD_MIRROR_IMAGE_SYNC_H

#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/journal/TypeTraits.h"
#include "common/Mutex.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include <map>
#include <vector>

class Context;
class ContextWQ;
namespace journal { class Journaler; }
namespace librbd { class ProgressContext; }
namespace librbd { template <typename> class DeepCopyRequest; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {

class ProgressContext;

template <typename> class InstanceWatcher;

template <typename ImageCtxT = librbd::ImageCtx>
class ImageSync : public BaseRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  static ImageSync* create(ImageCtxT *local_image_ctx,
                           ImageCtxT *remote_image_ctx,
                           const std::string &mirror_uuid,
                           Journaler *journaler,
                           MirrorPeerClientMeta *client_meta,
                           ContextWQ *work_queue,
                           InstanceWatcher<ImageCtxT> *instance_watcher,
                           Context *on_finish,
                           ProgressContext *progress_ctx = nullptr) {
    return new ImageSync(local_image_ctx, remote_image_ctx, mirror_uuid,
                         journaler, client_meta, work_queue, instance_watcher,
                         on_finish, progress_ctx);
  }

  ImageSync(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
            const std::string &mirror_uuid, Journaler *journaler,
            MirrorPeerClientMeta *client_meta, ContextWQ *work_queue,
            InstanceWatcher<ImageCtxT> *instance_watcher, Context *on_finish,
            ProgressContext *progress_ctx = nullptr);
  ~ImageSync() override;

  void send() override;
  void cancel() override;

protected:
  void finish(int r) override;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * NOTIFY_SYNC_REQUEST
   *    |
   *    v
   * PRUNE_CATCH_UP_SYNC_POINT
   *    |
   *    v
   * CREATE_SYNC_POINT (skip if already exists and
   *    |               not disconnected)
   *    v
   * SET_SYNC_POINT_SNAP_CONTEXT
   *    |
   *    v
   * COPY_IMAGE . . . . . . . . . . . . . .
   *    |                                 .
   *    v                                 .
   * UNSET_SYNC_POINT_SNAP_CONTEXT        .
   *    |                                 .
   *    v                                 .
   * PRUNE_SYNC_POINTS                    . (image sync canceled)
   *    |                                 .
   *    v                                 .
   * <finish> < . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;
  class ImageCopyProgressContext;

  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  std::string m_mirror_uuid;
  Journaler *m_journaler;
  MirrorPeerClientMeta *m_client_meta;
  ContextWQ *m_work_queue;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  ProgressContext *m_progress_ctx;

  SnapMap m_snap_map;

  Mutex m_lock;
  bool m_canceled = false;

  librbd::DeepCopyRequest<ImageCtxT> *m_image_copy_request = nullptr;
  librbd::ProgressContext *m_image_copy_prog_ctx = nullptr;

  void send_notify_sync_request();
  void handle_notify_sync_request(int r);

  void send_prune_catch_up_sync_point();
  void handle_prune_catch_up_sync_point(int r);

  void send_create_sync_point();
  void handle_create_sync_point(int r);

  void send_set_sync_point_snap_context();
  void handle_set_sync_point_snap_context(int r);

  void send_copy_image();
  void handle_copy_image(int r);

  void send_unset_sync_point_snap_context();
  void handle_unset_sync_point_snap_context(int r);

  void send_prune_sync_points();
  void handle_prune_sync_points(int r);

  void update_progress(const std::string &description);
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageSync<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_H
