// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/RemoveObjectMapRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RemoveObjectMapRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;

template <typename I>
RemoveObjectMapRequest<I>::RemoveObjectMapRequest(I *image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish),
    m_lock("RemoveObjectMapRequest::m_lock") {
}

template <typename I>
void RemoveObjectMapRequest<I>::send() {
  assert(m_image_ctx->snap_lock.is_wlocked());

  send_remove_object_map();
}

template <typename I>
void RemoveObjectMapRequest<I>::send_remove_object_map() {
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
    m_ref_counter++;
    std::string oid(ObjectMap::object_map_name(m_image_ctx->id, snap_id));
    using klass = RemoveObjectMapRequest<I>;
    librados::AioCompletion *comp =
      create_rados_ack_callback<klass, &klass::handle_remove_object_map>(this);

    int r = m_image_ctx->md_ctx.aio_remove(oid, comp);
    assert(r == 0);
    comp->release();
  }
}

template <typename I>
Context *RemoveObjectMapRequest<I>::handle_remove_object_map(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": r=" << *result << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_ref_counter > 0);
    m_ref_counter--;

    if (*result < 0 && *result != -ENOENT) {
      lderr(cct) << "failed to remove object map: " << cpp_strerror(*result)
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

template class librbd::image::RemoveObjectMapRequest<librbd::ImageCtx>;
