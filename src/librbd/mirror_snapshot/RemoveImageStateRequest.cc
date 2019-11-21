// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror_snapshot/RemoveImageStateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror_snapshot/Types.h"
#include "librbd/mirror_snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror_snapshot::RemoveImageStateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror_snapshot {

using librbd::util::create_rados_callback;

template <typename I>
void RemoveImageStateRequest<I>::send() {
  get_object_count();
}


template <typename I>
void RemoveImageStateRequest<I>::get_object_count() {
  CephContext *cct = m_image_ctx->cct;

  auto oid = util::image_state_object_name(m_image_ctx, m_snap_id, 0);
  ldout(cct, 20) << oid << dendl;

  librados::ObjectReadOperation op;
  op.read(0, ImageStateHeader::length(), &m_bl, nullptr);

  librados::AioCompletion *comp = create_rados_callback<
    RemoveImageStateRequest<I>,
    &RemoveImageStateRequest<I>::handle_get_object_count>(this);
  int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op, nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveImageStateRequest<I>::handle_get_object_count(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read image state object: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  ImageStateHeader header(1);
  try {
    using ceph::decode;
    decode(header, m_bl);
  } catch (const buffer::error &err) {
    lderr(cct) << "failed to decode image state object header" << dendl;
    // still try to remove it
  }
  m_object_count = header.object_count;

  remove_object();
}

template <typename I>
void RemoveImageStateRequest<I>::remove_object() {
  CephContext *cct = m_image_ctx->cct;

  auto oid = util::image_state_object_name(m_image_ctx, m_snap_id,
                                           m_object_index);
  ldout(cct, 20) << oid << dendl;

  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *comp = create_rados_callback<
    RemoveImageStateRequest<I>,
    &RemoveImageStateRequest<I>::handle_remove_object>(this);
  int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveImageStateRequest<I>::handle_remove_object(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to remove image state object: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  m_object_index++;

  if (m_object_index >= m_object_count) {
    finish(0);
    return;
  }

  remove_object();
}

template <typename I>
void RemoveImageStateRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror_snapshot
} // namespace librbd

template class librbd::mirror_snapshot::RemoveImageStateRequest<librbd::ImageCtx>;
