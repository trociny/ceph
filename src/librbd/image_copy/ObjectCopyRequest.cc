// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCopyRequest.h"
#include "librados/snap_set_diff.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "common/errno.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image_copy::ObjectCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librados {

bool operator==(const clone_info_t& rhs, const clone_info_t& lhs) {
  return (rhs.cloneid == lhs.cloneid &&
          rhs.snaps == lhs.snaps &&
          rhs.overlap == lhs.overlap &&
          rhs.size == lhs.size);
}

bool operator==(const snap_set_t& rhs, const snap_set_t& lhs) {
  return (rhs.clones == lhs.clones &&
          rhs.seq == lhs.seq);
}

} // namespace librados

namespace librbd {
namespace image_copy {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
ObjectCopyRequest<I>::ObjectCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                        const SnapMap &snap_map,
                                        uint64_t object_number,
                                        Context *on_finish)
  : m_src_image_ctx(src_image_ctx), m_dst_image_ctx(dst_image_ctx),
    m_cct(dst_image_ctx->cct), m_snap_map(snap_map),
    m_object_number(object_number), m_on_finish(on_finish) {
  assert(!snap_map.empty()); // XXXMG

  m_dst_io_ctx.dup(m_dst_image_ctx->data_ctx);
  m_dst_oid = m_dst_image_ctx->get_object_name(object_number);

  m_src_io_ctx.dup(m_src_image_ctx->data_ctx);
  m_src_oid = m_src_image_ctx->get_object_name(object_number);

  ldout(m_cct, 20) << "src_oid=" << m_src_oid << ", "
                   << "dst_oid=" << m_dst_oid << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::send() {
  send_list_snaps();
}

template <typename I>
void ObjectCopyRequest<I>::send_list_snaps() {
  ldout(m_cct, 20) << dendl;

  librados::AioCompletion *rados_completion = create_rados_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_list_snaps>(this);

  librados::ObjectReadOperation op;
  m_snap_set = {};
  m_snap_ret = 0;
  op.list_snaps(&m_snap_set, &m_snap_ret);

  m_src_io_ctx.snap_set_read(CEPH_SNAPDIR);
  int r = m_src_io_ctx.aio_operate(m_src_oid, rados_completion, &op,
                                      nullptr);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_list_snaps(int r) {
  if (r == 0 && m_snap_ret < 0) {
    r = m_snap_ret;
  }

  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    finish(0);
    return;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to list snaps: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_retry_missing_read) {
    if (m_snap_set == m_retry_snap_set) {
      lderr(m_cct) << "read encountered missing object using up-to-date snap set"
                   << dendl;
      finish(-ENOENT);
      return;
    }

    ldout(m_cct, 20) << "retrying using updated snap set" << dendl;
    m_retry_missing_read = false;
    m_retry_snap_set = {};
  }

  compute_diffs();
  send_read_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_read_object() {
  if (m_snap_copy_ops.empty()) {
    // no more snapshot diffs to read from source
    finish(0);
    return;
  }

  // build the read request
  auto &copy_ops = m_snap_copy_ops.begin()->second;
  assert(!copy_ops.empty());

  bool read_required = false;
  librados::ObjectReadOperation op;
  for (auto &copy_op : copy_ops) {
    switch (copy_op.type) {
    case COPY_OP_TYPE_WRITE:
      if (!read_required) {
        // map the copy op start snap id back to the necessary read snap id
        librados::snap_t src_snap_seq = m_snap_copy_ops.begin()->first.second;
        m_src_io_ctx.snap_set_read(src_snap_seq);

        ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << dendl;
        read_required = true;
      }
      ldout(m_cct, 20) << "read op: " << copy_op.offset << "~" << copy_op.length
                       << dendl;
      op.sparse_read(copy_op.offset, copy_op.length, &copy_op.extent_map,
                     &copy_op.out_bl, nullptr);
      op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                       LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      break;
    default:
      break;
    }
  }

  if (!read_required) {
    // nothing written to this object for this snapshot (must be trunc/remove)
    send_write_object();
    return;
  }

  librados::AioCompletion *comp = create_rados_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read_object>(this);
  int r = m_src_io_ctx.aio_operate(m_src_oid, comp, &op, nullptr);
  assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_read_object(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    m_retry_snap_set = m_snap_set;
    m_retry_missing_read = true;

    ldout(m_cct, 5) << "object missing potentially due to removed snapshot" << dendl;
    send_list_snaps();
    return;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to read from source object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  send_write_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_write_object() {
  // retrieve the destination snap context for the op
  SnapIds dst_snap_ids;
  librados::snap_t dst_snap_seq = 0;
  librados::snap_t src_snap_seq = m_snap_copy_ops.begin()->first.first;
  if (src_snap_seq != 0) {
    auto snap_map_it = m_snap_map.find(src_snap_seq);
    assert(snap_map_it != m_snap_map.end());

    // write snapshot context should be before actual snapshot
    if (snap_map_it != m_snap_map.begin()) {
      --snap_map_it;
      assert(!snap_map_it->second.empty());
      dst_snap_seq = snap_map_it->second.front();
      dst_snap_ids = snap_map_it->second;
    }
  }

  Context *finish_op_ctx;
  {
    RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
    finish_op_ctx = start_dst_op(m_dst_image_ctx->owner_lock);
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }

  ldout(m_cct, 20) << ""
           << "dst_snap_seq=" << dst_snap_seq << ", "
           << "dst_snaps=" << dst_snap_ids << dendl;

  auto &copy_ops = m_snap_copy_ops.begin()->second;
  assert(!copy_ops.empty());
  uint64_t object_offset;
  uint64_t buffer_offset;
  librados::ObjectWriteOperation op;
  for (auto &copy_op : copy_ops) {
    switch (copy_op.type) {
    case COPY_OP_TYPE_WRITE:
      object_offset = copy_op.offset;
      buffer_offset = 0;
      for (auto it : copy_op.extent_map) {
        if (object_offset < it.first) {
          ldout(m_cct, 20) << "zero op: " << object_offset << "~"
                   << it.first - object_offset << dendl;
          op.zero(object_offset, it.first - object_offset);
        }
        ldout(m_cct, 20) << "write op: " << it.first << "~" << it.second << dendl;
        bufferlist tmpbl;
        tmpbl.substr_of(copy_op.out_bl, buffer_offset, it.second);
        op.write(it.first, tmpbl);
        op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                         LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
        buffer_offset += it.second;
        object_offset = it.first + it.second;
      }
      if (object_offset < copy_op.offset + copy_op.length) {
        uint64_t copy_op_end = copy_op.offset + copy_op.length;
        assert(copy_op_end <= m_snap_object_sizes[src_snap_seq]);
        if (copy_op_end == m_snap_object_sizes[src_snap_seq]) {
          ldout(m_cct, 20) << "trunc op: " << object_offset << dendl;
          op.truncate(object_offset);
          m_snap_object_sizes[src_snap_seq] = object_offset;
        } else {
          ldout(m_cct, 20) << "zero op: " << object_offset << "~"
                   << copy_op_end - object_offset << dendl;
          op.zero(object_offset, copy_op_end - object_offset);
        }
      }
      break;
    case COPY_OP_TYPE_TRUNC:
      if (copy_op.offset > m_snap_object_sizes[src_snap_seq]) {
        // skip (must have been updated in WRITE op case issuing trunc op)
        break;
      }
      ldout(m_cct, 20) << "trunc op: " << copy_op.offset << dendl;
      op.truncate(copy_op.offset);
      break;
    case COPY_OP_TYPE_REMOVE:
      ldout(m_cct, 20) << "remove op" << dendl;
      op.remove();
      break;
    default:
      assert(false);
    }
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_write_object(r);
      finish_op_ctx->complete(0);
    });
  librados::AioCompletion *comp = create_rados_callback(ctx);
  int r = m_dst_io_ctx.aio_operate(m_dst_oid, comp, &op, dst_snap_seq,
                                   dst_snap_ids);
  assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_write_object(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  }
  if (r < 0) {
    lderr(m_cct) << "failed to write to destination object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  m_snap_copy_ops.erase(m_snap_copy_ops.begin());
  if (!m_snap_copy_ops.empty()) {
    send_read_object();
    return;
  }

  send_update_object_map();
}

template <typename I>
void ObjectCopyRequest<I>::send_update_object_map() {
  m_dst_image_ctx->owner_lock.get_read();
  m_dst_image_ctx->snap_lock.get_read();
  if (!m_dst_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP,
                                        m_dst_image_ctx->snap_lock) ||
      m_snap_object_states.empty()) {
    m_dst_image_ctx->snap_lock.put_read();
    m_dst_image_ctx->owner_lock.put_read();
    finish(0);
    return;
  } else if (m_dst_image_ctx->object_map == nullptr) {
    // possible that exclusive lock was lost in background
    lderr(m_cct) << "object map is not initialized" << dendl;

    m_dst_image_ctx->snap_lock.put_read();
    m_dst_image_ctx->owner_lock.put_read();
    finish(-EINVAL);
    return;
  }

  assert(m_dst_image_ctx->object_map != nullptr);

  auto snap_object_state = *m_snap_object_states.begin();
  m_snap_object_states.erase(m_snap_object_states.begin());

  ldout(m_cct, 20) << "dst_snap_id=" << snap_object_state.first << ", "
                   << "object_state="
                   << static_cast<uint32_t>(snap_object_state.second)
                   << dendl;

  auto finish_op_ctx = start_dst_op(m_dst_image_ctx->owner_lock);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    m_dst_image_ctx->snap_lock.put_read();
    m_dst_image_ctx->owner_lock.put_read();
    finish(-EROFS);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_update_object_map(r);
      finish_op_ctx->complete(0);
    });

  RWLock::WLocker object_map_locker(m_dst_image_ctx->object_map_lock);
  bool sent = m_dst_image_ctx->object_map->template aio_update<
    Context, &Context::complete>(
      snap_object_state.first, m_object_number, snap_object_state.second, {},
      {}, ctx);
  assert(sent);
  m_dst_image_ctx->snap_lock.put_read();
  m_dst_image_ctx->owner_lock.put_read();
}

template <typename I>
void ObjectCopyRequest<I>::handle_update_object_map(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  assert(r == 0);
  if (!m_snap_object_states.empty()) {
    send_update_object_map();
    return;
  }
  finish(0);
}

template <typename I>
Context *ObjectCopyRequest<I>::start_dst_op(RWLock &owner_lock) {
  assert(m_dst_image_ctx->owner_lock.is_locked());
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    // XXXMG: image might not have exclusive lock feature enabled
    return nullptr;
  }
  return m_dst_image_ctx->exclusive_lock->start_op();
}

template <typename I>
void ObjectCopyRequest<I>::compute_diffs() {
  m_snap_copy_ops = {};
  m_snap_object_states = {};
  m_snap_object_sizes = {};

  librados::snap_t src_copy_point_snap_id = m_snap_map.rbegin()->first;
  uint64_t prev_end_size = 0;
  bool prev_exists = false;
  librados::snap_t start_src_snap_id = 0;
  for (auto &pair : m_snap_map) {
    assert(!pair.second.empty());
    librados::snap_t end_src_snap_id = pair.first;
    librados::snap_t end_dst_snap_id = pair.second.front();

    interval_set<uint64_t> diff;
    uint64_t end_size;
    bool exists;
    librados::snap_t clone_end_snap_id;
    calc_snap_set_diff(m_cct, m_snap_set, start_src_snap_id,
                       end_src_snap_id, &diff, &end_size, &exists,
                       &clone_end_snap_id);

    ldout(m_cct, 20) << "start_src_snap_id=" << start_src_snap_id << ", "
                     << "end_src_snap_id=" << end_src_snap_id << ", "
                     << "clone_end_snap_id=" << clone_end_snap_id << ", "
                     << "end_dst_snap_id=" << end_dst_snap_id << ", "
                     << "diff=" << diff << ", "
                     << "end_size=" << end_size << ", "
                     << "exists=" << exists << dendl;
    if (exists) {
      // clip diff to size of object (in case it was truncated)
      if (end_size < prev_end_size) {
        interval_set<uint64_t> trunc;
        trunc.insert(end_size, prev_end_size);
        trunc.intersection_of(diff);
        diff.subtract(trunc);
        ldout(m_cct, 20) << "clearing truncate diff: " << trunc << dendl;
      }

      // prepare the object map state
      {
        RWLock::RLocker snap_locker(m_dst_image_ctx->snap_lock);
        uint8_t object_state = OBJECT_EXISTS;
        if (m_dst_image_ctx->test_features(RBD_FEATURE_FAST_DIFF,
                                           m_dst_image_ctx->snap_lock) &&
            prev_exists && diff.empty() && end_size == prev_end_size) {
          object_state = OBJECT_EXISTS_CLEAN;
        }
        m_snap_object_states[end_dst_snap_id] = object_state;
      }

      // reads should be issued against the newest (existing) snapshot within
      // the associated snapshot object clone. writes should be issued
      // against the oldest snapshot in the snap_map.
      assert(clone_end_snap_id >= end_src_snap_id);
      if (clone_end_snap_id > src_copy_point_snap_id) {
        // do not read past the copy point snapshot
        clone_end_snap_id = src_copy_point_snap_id;
      }

      // object write/zero, or truncate
      // NOTE: a single snapshot clone might represent multiple snapshots, but
      // the write/zero and truncate ops will only be associated with the first
      // snapshot encountered within the clone since the diff will be empty for
      // subsequent snapshots and the size will remain constant for a clone.
      for (auto it = diff.begin(); it != diff.end(); ++it) {
        ldout(m_cct, 20) << "read/write op: " << it.get_start() << "~"
                 << it.get_len() << dendl;
        m_snap_copy_ops[{end_src_snap_id, clone_end_snap_id}].emplace_back(
          COPY_OP_TYPE_WRITE, it.get_start(), it.get_len());
      }
      if (end_size < prev_end_size) {
        ldout(m_cct, 20) << "trunc op: " << end_size << dendl;
        m_snap_copy_ops[{end_src_snap_id, clone_end_snap_id}].emplace_back(
          COPY_OP_TYPE_TRUNC, end_size, 0U);
      }
      m_snap_object_sizes[end_src_snap_id] = end_size;
    } else {
      if (prev_exists) {
        // object remove
        ldout(m_cct, 20) << "remove op" << dendl;
        m_snap_copy_ops[{end_src_snap_id, end_src_snap_id}].emplace_back(
          COPY_OP_TYPE_REMOVE, 0U, 0U);
      }
    }

    prev_end_size = end_size;
    prev_exists = exists;
    start_src_snap_id = end_src_snap_id;
  }
}

template <typename I>
void ObjectCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  // ensure IoCtxs are closed prior to proceeding
  auto on_finish = m_on_finish;
  delete this;

  on_finish->complete(r);
}

} // namespace image_copy
} // namespace librbd

template class librbd::image_copy::ObjectCopyRequest<librbd::ImageCtx>;
