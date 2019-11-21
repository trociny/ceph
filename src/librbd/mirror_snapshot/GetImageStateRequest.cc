// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror_snapshot/GetImageStateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror_snapshot/Types.h"
#include "librbd/mirror_snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror_snapshot::GetImageStateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror_snapshot {

using librbd::util::create_rados_callback;

template <typename I>
void GetImageStateRequest<I>::send() {
  read_object();
}


template <typename I>
void GetImageStateRequest<I>::read_object() {
  CephContext *cct = m_image_ctx->cct;

  auto oid = util::image_state_object_name(m_image_ctx, m_snap_id,
                                           m_object_index);
  ldout(cct, 20) << oid << dendl;

  librados::ObjectReadOperation op;
  m_bl.clear();
  op.read(0, 0, &m_bl, nullptr);

  librados::AioCompletion *comp = create_rados_callback<
    GetImageStateRequest<I>,
    &GetImageStateRequest<I>::handle_read_object>(this);
  int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op, nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetImageStateRequest<I>::handle_read_object(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read image state object: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_object_index == 0) {
    ImageStateHeader header;
    bufferlist bl;
    bl.substr_of(m_bl, 0, header.length());
    try {
      using ceph::decode;
      decode(header, bl);
    } catch (const buffer::error &err) {
      lderr(cct) << "failed to decode image state object header" << dendl;
      finish(-EBADMSG);
      return;
    }
    m_object_count = header.object_count;
    m_state_bl.substr_of(m_bl, header.length(),
                         m_bl.length() - header.length());
  } else {
    m_state_bl.claim_append(m_bl);
  }

  m_object_index++;

  if (m_object_index >= m_object_count) {
    finish(0);
    return;
  }

  read_object();
}

template <typename I>
void GetImageStateRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    try {
      using ceph::decode;
      decode(*m_image_state, m_state_bl);
    } catch (const buffer::error &err) {
      lderr(cct) << "failed to decode image state" << dendl;
      r = -EBADMSG;
    }
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror_snapshot
} // namespace librbd

template class librbd::mirror_snapshot::GetImageStateRequest<librbd::ImageCtx>;
