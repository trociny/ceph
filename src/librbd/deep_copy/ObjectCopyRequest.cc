// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librados/snap_set_diff.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::ObjectCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librados {

inline bool operator==(const clone_info_t& rhs, const clone_info_t& lhs) {
  return (rhs.cloneid == lhs.cloneid &&
          rhs.snaps == lhs.snaps &&
          rhs.overlap == lhs.overlap &&
          rhs.size == lhs.size);
}

inline bool operator==(const snap_set_t& rhs, const snap_set_t& lhs) {
  return (rhs.clones == lhs.clones &&
          rhs.seq == lhs.seq);
}

} // namespace librados

namespace librbd {
namespace deep_copy {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
ObjectCopyRequest<I>::ObjectCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                        const SnapMap &snap_map,
                                        uint64_t dst_object_number,
                                        Context *on_finish)
  : m_src_image_ctx(src_image_ctx), m_dst_image_ctx(dst_image_ctx),
    m_cct(dst_image_ctx->cct), m_snap_map(snap_map),
    m_dst_object_number(dst_object_number), m_on_finish(on_finish) {
  assert(!m_snap_map.empty());

  m_src_io_ctx.dup(m_src_image_ctx->data_ctx);
  m_dst_io_ctx.dup(m_dst_image_ctx->data_ctx);

  m_dst_oid = m_dst_image_ctx->get_object_name(dst_object_number);

  ldout(m_cct, 20) << "dst_oid=" << m_dst_oid << dendl;

  compute_src_object_extents();
}

template <typename I>
void ObjectCopyRequest<I>::send() {
  send_list_snaps();
}

template <typename I>
void ObjectCopyRequest<I>::send_list_snaps() {
  assert(!m_src_objects.empty());
  m_src_ono = *m_src_objects.begin();
  m_src_oid = m_src_image_ctx->get_object_name(m_src_ono);

  ldout(m_cct, 20) << "src_oid=" << m_src_oid << dendl;

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
    r = 0;
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

  compute_read_ops();
  send_read_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_read_object() {

  if (m_read_snaps.empty()) {
    // all snapshots have been read
    merge_write_ops();

    assert(!m_src_objects.empty());
    m_src_objects.erase(m_src_objects.begin());

    if (!m_src_objects.empty()) {
      send_list_snaps();
      return;
    }

    // all objects have been read

    compute_zero_ops();

    if (m_write_ops.empty()) {
      // nothing to copy
      finish(0);
      return;
    }

    send_write_object();
    return;
  }

  auto index = *m_read_snaps.begin();
  auto src_snap_seq = index.second;

  bool read_required = false;
  librados::ObjectReadOperation op;

  for (auto &copy_op : m_read_ops[index]) {
    if (!read_required) {
      // map the copy op start snap id back to the necessary read snap id
      m_src_io_ctx.snap_set_read(src_snap_seq);

      ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << dendl;
      read_required = true;
    }
    ldout(m_cct, 20) << "read op: " << copy_op.src_offset << "~"
                     << copy_op.length << dendl;
    op.sparse_read(copy_op.src_offset, copy_op.length, &copy_op.src_extent_map,
                   &copy_op.out_bl, nullptr);
    op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                     LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
  }

  if (!read_required) {
    // nothing written to this object for this snapshot (must be trunc/remove)
    handle_read_object(0);
  }

  auto ctx = create_context_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read_object>(this);
  auto comp = create_rados_callback(ctx);

  ldout(m_cct, 20) << "read " << m_src_oid << dendl;

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

  assert(!m_read_snaps.empty());
  m_read_snaps.erase(m_read_snaps.begin());

  send_read_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_write_object() {
  ldout(m_cct, 20) << dendl;
  librados::ObjectWriteOperation op;

  assert(!m_write_ops.empty());

  // retrieve the destination snap context for the op
  SnapIds dst_snap_ids;
  librados::snap_t dst_snap_seq = 0;
  librados::snap_t src_snap_seq = m_write_ops.begin()->first;
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

  if (m_write_ops.begin()->second.empty()) {
    handle_write_object(0);
    return;
  }

  Context *finish_op_ctx;
  {
    RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
    finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock);
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }

  ldout(m_cct, 20) << "dst_snap_seq=" << dst_snap_seq << ", "
                   << "dst_snaps=" << dst_snap_ids << dendl;

  for (auto &copy_op : m_write_ops.begin()->second) {
    switch (copy_op.type) {
    case COPY_OP_TYPE_WRITE:
      ldout(m_cct, 20) << "write op: " << copy_op.dst_offset << "~"
                       << copy_op.length << dendl;
      op.write(copy_op.dst_offset, copy_op.out_bl);
      op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                       LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      break;
    case COPY_OP_TYPE_ZERO:
      ldout(m_cct, 20) << "zero op: " << copy_op.dst_offset << "~"
                       << copy_op.length << dendl;
      op.zero(copy_op.dst_offset, copy_op.length);
      break;
    case COPY_OP_TYPE_TRUNC:
      ldout(m_cct, 20) << "trunc op: " << copy_op.dst_offset << dendl;
      op.truncate(copy_op.dst_offset);
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

  m_write_ops.erase(m_write_ops.begin());
  if (!m_write_ops.empty()) {
    send_write_object();
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
      m_dst_object_state.empty()) {
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

  auto dst_object_state = *m_dst_object_state.begin();
  m_dst_object_state.erase(m_dst_object_state.begin());

  ldout(m_cct, 20) << "dst_snap_id=" << dst_object_state.first << ", "
                   << "object_state="
                   << static_cast<uint32_t>(dst_object_state.second) << dendl;

  auto finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock);
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
    Context, &Context::complete>(dst_object_state.first, m_dst_object_number,
                                 dst_object_state.second, {}, {}, ctx);
  assert(sent);
  m_dst_image_ctx->snap_lock.put_read();
  m_dst_image_ctx->owner_lock.put_read();
}

template <typename I>
void ObjectCopyRequest<I>::handle_update_object_map(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  assert(r == 0);
  if (!m_dst_object_state.empty()) {
    send_update_object_map();
    return;
  }
  finish(0);
}

template <typename I>
Context *ObjectCopyRequest<I>::start_lock_op(RWLock &owner_lock) {
  assert(m_dst_image_ctx->owner_lock.is_locked());
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    RWLock::RLocker snap_locker(m_dst_image_ctx->snap_lock);
    if (m_dst_image_ctx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK,
                                       m_dst_image_ctx->snap_lock)) {
      return nullptr;
    }
    return new FunctionContext([](int r) {});
  }
  return m_dst_image_ctx->exclusive_lock->start_op();
}

template <typename I>
void ObjectCopyRequest<I>::compute_src_object_extents() {
  std::vector<std::pair<uint64_t, uint64_t>> image_extents;
  Striper::extent_to_file(m_cct, &m_dst_image_ctx->layout, m_dst_object_number,
                          0, m_dst_image_ctx->layout.object_size, image_extents);

  size_t total = 0;
  for (auto &e : image_extents) {
    std::map<object_t, std::vector<ObjectExtent>> src_object_extents;
    Striper::file_to_extents(m_cct, m_src_image_ctx->format_string,
                             &m_src_image_ctx->layout, e.first, e.second, 0,
                             src_object_extents);
    for (auto &p : src_object_extents) {
      for (auto &s : p.second) {
        m_src_objects.insert(s.objectno);
        m_src_object_extents.push_back({s.objectno, s.offset, s.length});
        total += s.length;
      }
    }
  }

  assert(total == m_dst_image_ctx->layout.object_size);

  ldout(m_cct, 20) << m_src_object_extents.size() << " src extents" << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::compute_read_ops() {
  librados::snap_t src_copy_point_snap_id = m_snap_map.rbegin()->first;
  uint64_t prev_end_size = 0;
  bool prev_exists = false;
  librados::snap_t start_src_snap_id = 0;

  m_read_ops = {};
  m_read_snaps = {};
  m_zero_interval = {};
  m_end = {};

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

      // reads should be issued against the newest (existing) snapshot within
      // the associated snapshot object clone. writes should be issued
      // against the oldest snapshot in the snap_map.
      assert(clone_end_snap_id >= end_src_snap_id);
      if (clone_end_snap_id > src_copy_point_snap_id) {
        // do not read past the copy point snapshot
        clone_end_snap_id = src_copy_point_snap_id;
      }

      m_state[end_dst_snap_id] = OBJECT_EXISTS;
      // XXXMG: OBJECT_EXISTS_CLEAN?

      m_zero_interval[end_src_snap_id] = {};

      uint64_t dst_object_offset = 0;
      for (auto &e : m_src_object_extents) {
        if (e.object_no == m_src_ono) {
          // limit diff to read interval
          interval_set<uint64_t> read_interval;
          read_interval.insert(e.offset, e.length);
          read_interval.intersection_of(diff);

          ldout(m_cct, 20) << "read: " << read_interval << dendl;

          uint64_t src_object_offset = e.offset;
          //uint64_t dst_object_offset_end = dst_object_offset + e.length;
          if (e.offset <= end_size && e.offset + e.length > end_size) {
            auto len = end_size - e.offset;
            ldout(m_cct, 20) << "extent " << e.offset << "~" << e.length
                             << " intersects src object end " << end_size
                             << ", inserting zero " << dst_object_offset + len
                             << "~" << e.length - len << dendl;
            m_zero_interval[end_src_snap_id].insert(dst_object_offset + len,
                                                    e.length - len);
          } else if (e.offset >= end_size) {
            assert(read_interval.empty());
            ldout(m_cct, 20) << "extent " << e.offset << "~" << e.length
                             << " after src object end " << end_size
                             << ", inserting zero " << dst_object_offset << "~"
                             << e.length << dendl;
            m_zero_interval[end_src_snap_id].insert(dst_object_offset,
                                                    e.length);
          }
          m_end[end_src_snap_id] = std::max(dst_object_offset + e.length,
                                            m_end[end_src_snap_id]);
          for (auto it = read_interval.begin(); it != read_interval.end();
               it++) {
            assert(it.get_start() >= src_object_offset);
            auto offset = it.get_start() - src_object_offset;
            ldout(m_cct, 20) << "read/write op: " << it.get_start() << "~"
                             << it.get_len() << " dst: "
                             << dst_object_offset + offset << dendl;
            m_read_ops[{end_src_snap_id, clone_end_snap_id}]
              .emplace_back(COPY_OP_TYPE_WRITE, it.get_start(),
                            dst_object_offset + offset, it.get_len());
          }
        }
        dst_object_offset += e.length;
      }
    } else if (prev_exists) {
      uint64_t dst_object_offset = 0;
      for (auto &e : m_src_object_extents) {
        if (e.object_no == m_src_ono) {
          if (e.offset + e.length <= prev_end_size) {
            ldout(m_cct, 20) << "src object not exist and extent " << e.offset
                             << "~" << e.length << " inside object prev end "
                             << prev_end_size << ", inserting zero "
                             << dst_object_offset << "~" << e.length << dendl;
            m_zero_interval[end_src_snap_id].insert(dst_object_offset,
                                                    e.length);
            m_end[end_src_snap_id] = std::max(dst_object_offset + e.length,
                                              m_end[end_src_snap_id]);
          } else if (e.offset < prev_end_size) {
            auto len = prev_end_size - e.offset;
            ldout(m_cct, 20) << "src object not exist and extent " << e.offset
                             << "~" << e.length << " intersects object prev end"
                             << " " << prev_end_size << ", inserting zero "
                             << dst_object_offset << "~" << len << dendl;
            m_zero_interval[end_src_snap_id].insert(dst_object_offset, len);
            m_end[end_src_snap_id] = std::max(dst_object_offset + len,
                                              m_end[end_src_snap_id]);
          }
        }
        dst_object_offset += e.length;
      }
    }

    prev_end_size = end_size;
    prev_exists = exists;
    start_src_snap_id = end_src_snap_id;
  }

  for (auto &it : m_read_ops) {
    m_read_snaps.push_back(it.first);
  }
}

template <typename I>
void ObjectCopyRequest<I>::merge_write_ops() {
  ldout(m_cct, 20) << dendl;

  for (auto &it : m_zero_interval) {
    m_dst_zero_interval[it.first].insert(it.second);
  }

  for (auto &it : m_end) {
    m_dst_object_end[it.first] = std::max(m_dst_object_end[it.first],
                                          it.second);
  }

  m_dst_object_state.insert(m_state.begin(), m_state.end());

  for (auto &it : m_read_ops) {
    auto src_snap_seq = it.first.first;
    auto &copy_ops = it.second;
    for (auto &copy_op : copy_ops) {
      uint64_t src_offset = copy_op.src_offset;
      uint64_t dst_offset = copy_op.dst_offset;
      for (auto &e : copy_op.src_extent_map) {
        uint64_t zero_len = e.first - src_offset;
        if (zero_len > 0) {
          m_dst_zero_interval[src_snap_seq].insert(dst_offset, zero_len);
          src_offset += zero_len;
          dst_offset += zero_len;
        }
        copy_op.dst_extent_map[dst_offset] = e.second;
        src_offset += e.second;
        dst_offset += e.second;
      }
      m_write_ops[src_snap_seq].emplace_back(std::move(copy_op));
    }
  }
}

template <typename I>
void ObjectCopyRequest<I>::compute_zero_ops() {
  ldout(m_cct, 20) << dendl;

  uint64_t prev_end_size = 0;
  for (auto &it : m_dst_zero_interval) {
    auto src_snap_seq = it.first;
    auto &zero_interval = it.second;

    ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", prev_end_size="
                     << prev_end_size << dendl;

    if (zero_interval.empty()) {
      prev_end_size = std::max(prev_end_size, m_dst_object_end[src_snap_seq]);
      continue;
    }

    assert(m_dst_object_end.count(src_snap_seq) > 0);

    uint64_t end_size = m_dst_object_end[src_snap_seq];
    for (auto z = zero_interval.begin(); z != zero_interval.end(); z++) {
      if (z.get_start() + z.get_len() == m_dst_object_end[src_snap_seq]) {
        // zero interval at the object end
        if (z.get_start() < prev_end_size) {
          if (z.get_start() == 0) {
            m_write_ops[src_snap_seq]
              .emplace_back(COPY_OP_TYPE_REMOVE, 0, 0, 0);
            ldout(m_cct, 20) << "COPY_OP_TYPE_REMOVE" << dendl;
          } else {
            m_write_ops[src_snap_seq]
              .emplace_back(COPY_OP_TYPE_TRUNC, 0, z.get_start(), 0);
            ldout(m_cct, 20) << "COPY_OP_TYPE_TRUNC " << z.get_start() << dendl;
          }
        }
        end_size = z.get_start();
      } else {
        // zero interval inside the object
        assert(z.get_start() + z.get_len() < m_dst_object_end[src_snap_seq]);
        m_write_ops[src_snap_seq]
          .emplace_back(COPY_OP_TYPE_ZERO, 0, z.get_start(), z.get_len());
        ldout(m_cct, 20) << "COPY_OP_TYPE_ZERO " << z.get_start() << "~"
                         << z.get_len() << dendl;
      }
    }
    ldout(m_cct, 20) << "end_size=" << end_size << dendl;
    if (end_size == 0) {
      m_dst_object_state.erase(src_snap_seq);
    }
    prev_end_size = end_size;
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

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;
