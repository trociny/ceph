// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/SetFlagsRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::SetFlagsRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;

template <typename I>
SetFlagsRequest<I>::SetFlagsRequest(I *image_ctx, uint64_t flags,
				    uint64_t mask, Context *on_finish)
  : m_image_ctx(image_ctx), m_flags(flags), m_mask(mask),
    m_on_finish(on_finish), m_lock("SetFlagsRequest::m_lock") {
}

template <typename I>
void SetFlagsRequest<I>::send() {
  assert(m_image_ctx->snap_lock.is_wlocked());

  send_set_flags();
}

template <typename I>
void SetFlagsRequest<I>::send_set_flags() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  std::vector<uint64_t> snap_ids;
  snap_ids.push_back(CEPH_NOSNAP);
  for (auto it : m_image_ctx->snap_info) {
    snap_ids.push_back(it.first);
  }

  Mutex::Locker locker(m_lock);
  assert(m_ref_counter == 0);

  for (auto snap_id : snap_ids) {
    librados::ObjectWriteOperation op;
    cls_client::set_flags(&op, snap_id, m_flags, m_mask);

    using klass = SetFlagsRequest<I>;
    librados::AioCompletion *comp =
      create_rados_ack_callback<klass, &klass::handle_set_flags>(this);
    int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op);
    assert(r == 0);
    comp->release();
    m_ref_counter++;
  }
}

template <typename I>
Context *SetFlagsRequest<I>::handle_set_flags(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": r=" << *result << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_ref_counter > 0);
    m_ref_counter--;

    if (*result < 0 && *result != -ENOENT) {
      lderr(cct) << "failed to update image flags: " << cpp_strerror(*result)
		 << dendl;
      m_error_result = *result;
    }
    if (m_ref_counter > 0) {
      return nullptr;
    }
  }
  if (m_error_result < 0) {
    *result = m_error_result;
  }
  return m_on_finish;
}

} // namespace image
} // namespace librbd

template class librbd::image::SetFlagsRequest<librbd::ImageCtx>;
