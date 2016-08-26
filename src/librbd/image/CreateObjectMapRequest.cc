// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/CreateObjectMapRequest.h"
#include "include/assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::CreateObjectMapRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;

template <typename I>
CreateObjectMapRequest<I>::CreateObjectMapRequest(I *image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish),
    m_lock("CreateObjectMapRequest::m_lock") {
}

template <typename I>
void CreateObjectMapRequest<I>::send() {
  CephContext *cct = m_image_ctx->cct;
  assert(m_image_ctx->snap_lock.is_wlocked());

  uint64_t max_size = m_image_ctx->size;
  m_snap_ids.push_back(CEPH_NOSNAP);
  for (auto it : m_image_ctx->snap_info) {
    max_size = MAX(max_size, it.second.size);
    m_snap_ids.push_back(it.first);
  }
  if (!ObjectMap::is_compatible(m_image_ctx->layout, max_size)) {
    lderr(cct) << "image size not compatible with object map" << dendl;
    m_on_finish->complete(-EINVAL);
    return;
  }

  send_object_map_resize();
}

template <typename I>
void CreateObjectMapRequest<I>::send_object_map_resize() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_ref_counter == 0);

  for (auto snap_id : m_snap_ids) {
    m_ref_counter++;
    librados::ObjectWriteOperation op;
    uint64_t snap_size = m_image_ctx->get_image_size(snap_id);

    cls_client::object_map_resize(&op, Striper::get_num_objects(
				    m_image_ctx->layout, snap_size),
				  OBJECT_NONEXISTENT);

    std::string oid(ObjectMap::object_map_name(m_image_ctx->id, snap_id));
    using klass = CreateObjectMapRequest<I>;
    librados::AioCompletion *comp =
      create_rados_ack_callback<klass, &klass::handle_object_map_resize>(this);
    int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op);
    assert(r == 0);
    comp->release();
  }
}

template <typename I>
Context *CreateObjectMapRequest<I>::handle_object_map_resize(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": r=" << *result << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_ref_counter > 0);
    m_ref_counter--;

    if (*result < 0 && *result != -ENOENT) {
      lderr(cct) << "failed to create object map: " << cpp_strerror(*result)
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

template class librbd::image::CreateObjectMapRequest<librbd::ImageCtx>;
