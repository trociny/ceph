// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageCopyRequest.h"
#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ProgressContext.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image_copy::ImageCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace image_copy {

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
ImageCopyRequest<I>::ImageCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                      const SnapSeqs &snap_seqs,
                                      ProgressContext *prog_ctx,
                                      Context *on_finish)
  : RefCountedObject(dst_image_ctx->cct, 1), m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_snap_seqs(snap_seqs),
    m_prog_ctx(prog_ctx), m_on_finish(on_finish), m_cct(dst_image_ctx->cct),
    m_lock(unique_lock_name("ImageCopyRequest::m_lock", this)) {
}

template <typename I>
void ImageCopyRequest<I>::send() {
  int r = compute_snap_map();
  if (r < 0) {
    finish(r);
    return;
  }

  send_object_copies();
}

template <typename I>
void ImageCopyRequest<I>::cancel() {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << dendl;
  m_canceled = true;
}

template <typename I>
void ImageCopyRequest<I>::send_object_copies() {
  m_object_no = 0;
  {
    RWLock::RLocker snap_locker(m_src_image_ctx->snap_lock);
    m_end_object_no =  m_src_image_ctx->get_object_count(CEPH_NOSNAP);
    for (auto snap_id : m_src_image_ctx->snaps) {
      m_end_object_no = std::max(m_end_object_no,
                                 m_src_image_ctx->get_object_count(snap_id));
    }
  }

  ldout(m_cct, 20) << "start_object=" << m_object_no << ", "
                   << "end_object=" << m_end_object_no << dendl;

  bool complete;
  {
    Mutex::Locker locker(m_lock);
    for (int i = 0; i < m_cct->_conf->rbd_concurrent_management_ops; ++i) {
      send_next_object_copy();
      if (m_ret_val < 0 && m_current_ops == 0) {
        break;
      }
    }
    complete = (m_current_ops == 0);
  }

  if (complete) {
    finish(m_ret_val);
  }
}

template <typename I>
void ImageCopyRequest<I>::send_next_object_copy() {
  assert(m_lock.is_locked());

  if (m_canceled && m_ret_val == 0) {
    ldout(m_cct, 10) << "image copy canceled" << dendl;
    m_ret_val = -ECANCELED;
  }

  if (m_ret_val < 0 || m_object_no >= m_end_object_no) {
    return;
  }

  uint64_t ono = m_object_no++;

  ldout(m_cct, 20) << "object_num=" << ono << dendl;

  ++m_current_ops;

  Context *ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_object_copy>(this);
  ObjectCopyRequest<I> *req = ObjectCopyRequest<I>::create(
    m_src_image_ctx, m_dst_image_ctx, m_snap_map, ono, ctx);
  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_object_copy(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  bool complete;
  {
    Mutex::Locker locker(m_lock);
    assert(m_current_ops > 0);
    --m_current_ops;

    m_prog_ctx->update_progress(m_object_no, m_end_object_no);

    if (r < 0) {
      lderr(m_cct) << "object copy failed: " << cpp_strerror(r) << dendl;
      if (m_ret_val == 0) {
        m_ret_val = r;
      }
    }

    send_next_object_copy();
    complete = (m_current_ops == 0);
  }

  if (complete) {
    finish(m_ret_val);
  }
}

template <typename I>
int ImageCopyRequest<I>::compute_snap_map() {
  ldout(m_cct, 20) << dendl;

  librados::snap_t snap_id_start = 0; // XXXMG
  librados::snap_t snap_id_end;
  {
    RWLock::RLocker snap_locker(m_src_image_ctx->snap_lock);
    snap_id_end = m_src_image_ctx->snap_id;
  }

  SnapIds snap_ids;
  for (auto it : m_snap_seqs) {
    snap_ids.insert(snap_ids.begin(), it.second);
    if (it.first < snap_id_start) {
      continue;
    } else if (snap_id_end != CEPH_NOSNAP && it.first > snap_id_end) {
      break;
    }

    m_snap_map[it.first] = snap_ids;
  }

  if (m_snap_map.empty()) {
    // XXXMG: do we want to copy an image without snapshots?
    lderr(m_cct) << "failed to map snapshots within boundary" << dendl;
    return -EINVAL;
  }

  return 0;
}

template <typename I>
void ImageCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  put();
}

} // namespace image_copy
} // namespace librbd

template class librbd::image_copy::ImageCopyRequest<librbd::ImageCtx>;
