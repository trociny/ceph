// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/MirrorEnableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::MirrorEnableRequest: "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_ack_callback;

template <typename I>
MirrorEnableRequest<I>::MirrorEnableRequest(librados::IoCtx *io_ctx,
					    const std::string &image_id,
					    Context *on_finish)
  : m_io_ctx(io_ctx), m_image_id(image_id) {
}

template <typename I>
MirrorEnableRequest<I>::MirrorEnableRequest(I *image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish) {
}

template <typename I>
void MirrorEnableRequest<I>::send() {
  CephContext *cct = m_image_ctx->cct;

  if (m_image_ctx != nullptr) {
    if ((m_image_ctx->features & RBD_FEATURE_JOURNALING) == 0) {
      lderr(cct) << "journaling is not enabled" << dendl;
      m_on_finish->complete(-EINVAL);
      return;
    }
    send_get_tag_owner();
    return;
  }

  send_open_image();
}

template <typename I>
void MirrorEnableRequest<I>::send_open_image() {
  assert(m_image_ctx == nullptr);
  m_image_ctx = I::create("", m_image_id, nullptr, *m_io_ctx, false);

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    MirrorEnableRequest<I>,
    &MirrorEnableRequest<I>::handle_open_image>(this);

  m_image_ctx->state->open(ctx);
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_open_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(*result)
	       << dendl;
    delete m_image_ctx;
    return handle_finish(result);
  }

  m_close_image_on_finish = true;

  if ((m_image_ctx->features & RBD_FEATURE_JOURNALING) == 0) {
    lderr(cct) << "journaling is not enabled" << dendl;
    *result = -EINVAL;
    return handle_finish(result);
  }

  send_get_tag_owner();
  return nullptr;
}

template <typename I>
void MirrorEnableRequest<I>::send_get_tag_owner() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = MirrorEnableRequest<I>;
  Context *ctx = create_context_callback<
      klass, &klass::handle_get_tag_owner>(this);

  librbd::Journal<>::is_tag_owner(m_image_ctx, &m_is_primary, ctx);
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_get_tag_owner(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to check tag ownership: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(result);
  }

  if (!m_is_primary) {
    lderr(cct) << "last journal tag not owned by local cluster" << dendl;
    *result = -EINVAL;
    return handle_finish(result);
  }

  send_get_mirror_image();
  return nullptr;
}

template <typename I>
void MirrorEnableRequest<I>::send_get_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx->id);

  using klass = MirrorEnableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_get_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_get_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    *result = cls_client::mirror_image_get_finish(&iter, &m_mirror_image);
  }

  if (*result == 0) {
    if (m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
      ldout(cct, 10) << this << " " << __func__
		     << ": mirroring is already enabled" << dendl;
    } else {
      lderr(cct) << "currently disabling" << dendl;
      *result = -EINVAL;
    }
    return handle_finish(result);
  }

  if (*result != -ENOENT) {
    lderr(cct) << "failed to retreive mirror image: " << cpp_strerror(*result)
	       << dendl;
    return handle_finish(result);
  }

  *result = 0;
  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_mirror_image.global_image_id = uuid_gen.to_string();

  send_set_mirror_image();
  return nullptr;
}

template <typename I>
void MirrorEnableRequest<I>::send_set_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_set(&op, m_image_ctx->id, m_mirror_image);

  using klass = MirrorEnableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_set_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_set_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to enable mirroring: " << cpp_strerror(*result)
	       << dendl;
    return handle_finish(result);
  }

  send_notify_mirroring_watcher();
  return nullptr;
}

template <typename I>
void MirrorEnableRequest<I>::send_notify_mirroring_watcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = MirrorEnableRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<>::notify_image_updated(m_image_ctx->md_ctx,
					   cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
					   m_image_ctx->id,
					   m_mirror_image.global_image_id, ctx);
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_notify_mirroring_watcher(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to send update notification: "
	       << cpp_strerror(*result) << dendl;
    *result = 0;
  }

  return handle_finish(result);
}

template <typename I>
void MirrorEnableRequest<I>::send_close_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    MirrorEnableRequest<I>,
    &MirrorEnableRequest<I>::handle_close_image>(this);

  m_image_ctx->state->close(ctx);
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_close_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  return handle_finish(result);
}

template <typename I>
Context *MirrorEnableRequest<I>::handle_finish(int *result) {
  if (m_close_image_on_finish) {
    m_close_image_on_finish = false;
    m_error_result = *result;
    send_close_image();
    return nullptr;
  }
  if (m_error_result < 0) {
    *result = m_error_result;
  }
  return m_on_finish;
}

} // namespace image
} // namespace librbd

template class librbd::image::MirrorEnableRequest<librbd::ImageCtx>;
