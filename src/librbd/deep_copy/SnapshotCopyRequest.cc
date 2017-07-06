// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SnapshotCopyRequest.h"
#include "SnapshotCreateRequest.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::SnapshotCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

namespace {

template <typename I>
const std::string &get_snapshot_name(I *image_ctx, librados::snap_t snap_id) {
  auto snap_it = std::find_if(image_ctx->snap_ids.begin(),
                              image_ctx->snap_ids.end(),
                              [snap_id](
				const std::pair<
					  std::pair<cls::rbd::SnapshotNamespace,
						    std::string>,
					  librados::snap_t> &pair) {
    return pair.second == snap_id;
  });
  assert(snap_it != image_ctx->snap_ids.end());
  return snap_it->first.second;
}

} // anonymous namespace

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
SnapshotCopyRequest<I>::SnapshotCopyRequest(I *src_image_ctx,
                                            I *dst_image_ctx,
                                            ContextWQ *work_queue,
                                            SnapSeqs *snap_seqs,
                                            Context *on_finish)
  : RefCountedObject(dst_image_ctx->cct, 1), m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_work_queue(work_queue),
    m_snap_seqs(snap_seqs), m_on_finish(on_finish), m_cct(dst_image_ctx->cct),
    m_lock(unique_lock_name("SnapshotCopyRequest::m_lock", this)) {
  // snap ids ordered from oldest to newest
  m_src_snap_ids.insert(src_image_ctx->snaps.begin(),
                        src_image_ctx->snaps.end());
  m_dst_snap_ids.insert(dst_image_ctx->snaps.begin(),
                        dst_image_ctx->snaps.end());
}

template <typename I>
void SnapshotCopyRequest<I>::send() {
  librbd::ParentSpec src_parent_spec;
  int r = validate_parent(m_src_image_ctx, &src_parent_spec);
  if (r < 0) {
    lderr(m_cct) << "source image parent spec mismatch" << dendl;
    error(r);
    return;
  }

  r = validate_parent(m_dst_image_ctx, &m_dst_parent_spec);
  if (r < 0) {
    lderr(m_cct) << "destination image parent spec mismatch" << dendl;
    error(r);
    return;
  }

  send_snap_unprotect();
}

template <typename I>
void SnapshotCopyRequest<I>::cancel() {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << dendl;
  m_canceled = true;
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_unprotect() {

  SnapIdSet::iterator snap_id_it = m_dst_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_dst_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_dst_snap_ids.end(); ++snap_id_it) {
    librados::snap_t dst_snap_id = *snap_id_it;

    m_dst_image_ctx->snap_lock.get_read();

    bool dst_unprotected;
    int r = m_dst_image_ctx->is_snap_unprotected(dst_snap_id, &dst_unprotected);
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve destination snap unprotect status: "
           << cpp_strerror(r) << dendl;
      m_dst_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_dst_image_ctx->snap_lock.put_read();

    if (dst_unprotected) {
      // snap is already unprotected -- check next snap
      continue;
    }

    // if destination snapshot is protected and (1) it isn't in our mapping
    // table, or (2) the source snapshot isn't protected, unprotect it
    auto snap_seq_it = std::find_if(
      m_snap_seqs->begin(), m_snap_seqs->end(),
      [dst_snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == dst_snap_id;
      });

    if (snap_seq_it != m_snap_seqs->end()) {
      m_src_image_ctx->snap_lock.get_read();
      bool src_unprotected;
      r = m_src_image_ctx->is_snap_unprotected(snap_seq_it->first,
                                               &src_unprotected);
      if (r < 0) {
        lderr(m_cct) << "failed to retrieve source snap unprotect status: "
                     << cpp_strerror(r) << dendl;
        m_src_image_ctx->snap_lock.put_read();
        finish(r);
        return;
      }
      m_src_image_ctx->snap_lock.put_read();

      if (src_unprotected) {
        // source is unprotected -- unprotect destination snap
        break;
      }
    } else {
      // source snapshot doesn't exist -- unprotect destination snap
      break;
    }
  }

  if (snap_id_it == m_dst_snap_ids.end()) {
    // no destination snapshots to unprotect
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_remove();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_dst_image_ctx, m_prev_snap_id);

  ldout(m_cct, 20) << "snap_name=" << m_snap_name << ", "
                   << "snap_id=" << m_prev_snap_id << dendl;

  auto finish_op_ctx = start_dst_op();
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_snap_unprotect(r);
      finish_op_ctx->complete(0);
    });
  RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
  m_dst_image_ctx->operations->execute_snap_unprotect(
    cls::rbd::UserSnapshotNamespace(), m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_unprotect(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unprotect snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  if (handle_cancellation())
  {
    return;
  }

  send_snap_unprotect();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_remove() {
  SnapIdSet::iterator snap_id_it = m_dst_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_dst_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_dst_snap_ids.end(); ++snap_id_it) {
    librados::snap_t dst_snap_id = *snap_id_it;

    cls::rbd::SnapshotNamespace snap_namespace;
    m_dst_image_ctx->snap_lock.get_read();
    int r = m_dst_image_ctx->get_snap_namespace(dst_snap_id, &snap_namespace);
    m_dst_image_ctx->snap_lock.put_read();
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve destination snap namespace: "
                   << m_snap_name << dendl;
      finish(r);
      return;
    }

    if (boost::get<cls::rbd::UserSnapshotNamespace>(&snap_namespace) ==
          nullptr) {
      continue;
    }

    // if the destination snapshot isn't in our mapping table, remove it
    auto snap_seq_it = std::find_if(
      m_snap_seqs->begin(), m_snap_seqs->end(),
      [dst_snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == dst_snap_id;
      });

    if (snap_seq_it == m_snap_seqs->end()) {
      break;
    }
  }

  if (snap_id_it == m_dst_snap_ids.end()) {
    // no destination snapshots to delete
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_create();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_dst_image_ctx, m_prev_snap_id);

  ldout(m_cct, 20) << ""
           << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  auto finish_op_ctx = start_dst_op();
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_snap_remove(r);
      finish_op_ctx->complete(0);
    });
  RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
  m_dst_image_ctx->operations->execute_snap_remove(
    cls::rbd::UserSnapshotNamespace(), m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_remove(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove snapshot '" << m_snap_name << "': "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  if (handle_cancellation())
  {
    return;
  }

  send_snap_remove();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_create() {
  SnapIdSet::iterator snap_id_it = m_src_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_src_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_src_snap_ids.end(); ++snap_id_it) {
    librados::snap_t src_snap_id = *snap_id_it;

    cls::rbd::SnapshotNamespace snap_namespace;
    m_src_image_ctx->snap_lock.get_read();
    int r = m_src_image_ctx->get_snap_namespace(src_snap_id, &snap_namespace);
    m_src_image_ctx->snap_lock.put_read();
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve source snap namespace: "
                   << m_snap_name << dendl;
      finish(r);
      return;
    }

    // if the source snapshot isn't in our mapping table, create it
    if (m_snap_seqs->find(src_snap_id) == m_snap_seqs->end() &&
	boost::get<cls::rbd::UserSnapshotNamespace>(&snap_namespace) != nullptr) {
      break;
    }
  }

  if (snap_id_it == m_src_snap_ids.end()) {
    // no source snapshots to create
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_protect();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_src_image_ctx, m_prev_snap_id);

  m_src_image_ctx->snap_lock.get_read();
  auto snap_info_it = m_src_image_ctx->snap_info.find(m_prev_snap_id);
  if (snap_info_it == m_src_image_ctx->snap_info.end()) {
    m_src_image_ctx->snap_lock.put_read();
    lderr(m_cct) << "failed to retrieve source snap info: " << m_snap_name
                 << dendl;
    finish(-ENOENT);
    return;
  }

  uint64_t size = snap_info_it->second.size;
  m_snap_namespace = snap_info_it->second.snap_namespace;
  librbd::ParentSpec parent_spec;
  uint64_t parent_overlap = 0;
  if (snap_info_it->second.parent.spec.pool_id != -1) {
    parent_spec = m_dst_parent_spec;
    parent_overlap = snap_info_it->second.parent.overlap;
  }
  m_src_image_ctx->snap_lock.put_read();


  ldout(m_cct, 20) << "snap_name=" << m_snap_name << ", "
                   << "snap_id=" << m_prev_snap_id << ", "
                   << "size=" << size << ", "
                   << "parent_info=["
                   << "pool_id=" << parent_spec.pool_id << ", "
                   << "image_id=" << parent_spec.image_id << ", "
                   << "snap_id=" << parent_spec.snap_id << ", "
                   << "overlap=" << parent_overlap << "]" << dendl;

  Context *finish_op_ctx = start_dst_op();
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_snap_create(r);
      finish_op_ctx->complete(0);
    });
  SnapshotCreateRequest<I> *req = SnapshotCreateRequest<I>::create(
    m_dst_image_ctx, m_snap_name, m_snap_namespace, size, parent_spec,
    parent_overlap, ctx);
  req->send();
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_create(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  if (handle_cancellation())
  {
    return;
  }

  assert(m_prev_snap_id != CEPH_NOSNAP);

  auto snap_it = m_dst_image_ctx->snap_ids.find(
      {cls::rbd::UserSnapshotNamespace(), m_snap_name});
  assert(snap_it != m_dst_image_ctx->snap_ids.end());
  librados::snap_t dst_snap_id = snap_it->second;

  ldout(m_cct, 20) << "mapping source snap id " << m_prev_snap_id << " to "
                   << dst_snap_id << dendl;
  (*m_snap_seqs)[m_prev_snap_id] = dst_snap_id;

  send_snap_create();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_protect() {
  SnapIdSet::iterator snap_id_it = m_src_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_src_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_src_snap_ids.end(); ++snap_id_it) {
    librados::snap_t src_snap_id = *snap_id_it;

    m_src_image_ctx->snap_lock.get_read();

    bool src_protected;
    int r = m_src_image_ctx->is_snap_protected(src_snap_id, &src_protected);
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve source snap protect status: "
                   << cpp_strerror(r) << dendl;
      m_src_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_src_image_ctx->snap_lock.put_read();

    if (!src_protected) {
      // snap is not protected -- check next snap
      continue;
    }

    // if destination snapshot is not protected, protect it
    auto snap_seq_it = m_snap_seqs->find(src_snap_id);
    assert(snap_seq_it != m_snap_seqs->end());

    m_dst_image_ctx->snap_lock.get_read();
    bool dst_protected;
    r = m_dst_image_ctx->is_snap_protected(snap_seq_it->second, &dst_protected);
    if (r < 0) {
      lderr(m_cct) << "failed to retrieve destination snap protect status: "
                   << cpp_strerror(r) << dendl;
      m_dst_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_dst_image_ctx->snap_lock.put_read();

    if (!dst_protected) {
      break;
    }
  }

  if (snap_id_it == m_src_snap_ids.end()) {
    // no destination snapshots to protect
    m_prev_snap_id = CEPH_NOSNAP;
    finish(0);
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_src_image_ctx, m_prev_snap_id);

  ldout(m_cct, 20) << "snap_name=" << m_snap_name << ", "
                   << "snap_id=" << m_prev_snap_id << dendl;

  auto finish_op_ctx = start_dst_op();
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_snap_protect(r);
      finish_op_ctx->complete(0);
    });
  RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
  m_dst_image_ctx->operations->execute_snap_protect(
    cls::rbd::UserSnapshotNamespace(), m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_protect(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to protect snapshot '" << m_snap_name << "': "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  if (handle_cancellation())
  {
    return;
  }

  send_snap_protect();
}

template <typename I>
bool SnapshotCopyRequest<I>::handle_cancellation() {
  {
    Mutex::Locker locker(m_lock);
    if (!m_canceled) {
      return false;
    }
  }
  ldout(m_cct, 10) << "snapshot copy canceled" << dendl;
  finish(-ECANCELED);
  return true;
}

template <typename I>
void SnapshotCopyRequest<I>::error(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_work_queue->queue(new FunctionContext([this, r](int r1) { finish(r); }));
}

template <typename I>
int SnapshotCopyRequest<I>::validate_parent(I *image_ctx,
                                            librbd::ParentSpec *spec) {
  RWLock::RLocker owner_locker(image_ctx->owner_lock);
  RWLock::RLocker snap_locker(image_ctx->snap_lock);

  // ensure source image's parent specs are still consistent
  *spec = image_ctx->parent_md.spec;
  for (auto &snap_info_pair : image_ctx->snap_info) {
    auto &parent_spec = snap_info_pair.second.parent.spec;
    if (parent_spec.pool_id == -1) {
      continue;
    } else if (spec->pool_id == -1) {
      *spec = parent_spec;
      continue;
    }

    if (*spec != parent_spec) {
      return -EINVAL;
    }
  }
  return 0;
}

template <typename I>
Context *SnapshotCopyRequest<I>::start_dst_op() {
  RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    if (m_dst_image_ctx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
      return nullptr;
    }
    return new FunctionContext([](int r) {});
  }
  return m_dst_image_ctx->exclusive_lock->start_op();
}

template <typename I>
void SnapshotCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  put();
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::SnapshotCopyRequest<librbd::ImageCtx>;
