// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include <list>
#include <map>
#include <string>
#include <vector>

class Context;
class RWLock;

namespace librbd {
namespace deep_copy {

template <typename ImageCtxT = librbd::ImageCtx>
class ObjectCopyRequest {
public:
  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  static ObjectCopyRequest* create(ImageCtxT *src_image_ctx,
                                   ImageCtxT *dst_image_ctx,
                                   const SnapMap &snap_map,
                                   uint64_t object_number, Context *on_finish) {
    return new ObjectCopyRequest(src_image_ctx, dst_image_ctx, snap_map,
                                 object_number, on_finish);
  }

  ObjectCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                    const SnapMap &snap_map, uint64_t object_number,
                    Context *on_finish);

  void send();

  // testing support
  inline librados::IoCtx &get_src_io_ctx() {
    return m_src_io_ctx;
  }
  inline librados::IoCtx &get_dst_io_ctx() {
    return m_dst_io_ctx;
  }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_SNAPS < * * *
   *    | |      ^    * (-ENOENT and snap set stale)
   *    | |      |    *
   *    | \------/ (repeat for each src object)
   *    |             *
   *    |   * * * * * *
   *    v   *
   * READ_OBJECT <--------\
   *    | |      ^        | (repeat for each snapshot)
   *    | |      |        |
   *    | \------/ (repeat for each src object)
   *    v                 |
   * WRITE_OBJECT --------/
   *    |
   *    |     /-----------\
   *    |     |           | (repeat for each snapshot)
   *    v     v           |
   * UPDATE_OBJECT_MAP ---/ (skip if object
   *    |                    map disabled)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  enum CopyOpType {
    COPY_OP_TYPE_WRITE,
    COPY_OP_TYPE_ZERO,
    COPY_OP_TYPE_TRUNC,
    COPY_OP_TYPE_REMOVE
  };

  typedef std::map<uint64_t, uint64_t> ExtentMap;

  struct CopyOp {
    CopyOp(CopyOpType type, uint64_t offset, uint64_t length)
      : type(type), offset(offset), length(length) {
    }

    CopyOpType type;
    uint64_t offset;
    uint64_t length;

    ExtentMap extent_map;
    bufferlist out_bl;
  };

  typedef std::map<uint64_t, std::vector<std::pair<uint64_t, size_t>>> ObjectOffsets; // src_object_id -> list of (object_off, len)
  typedef std::list<CopyOp> ObjectCopyOps;
  typedef std::pair<librados::snap_t, librados::snap_t> WriteReadSnapIds;
  typedef std::map<uint64_t, ObjectCopyOps> CopyOps;  // src_object_id -> object_copy_ops
  typedef std::map<WriteReadSnapIds, CopyOps> SnapCopyOps;
  typedef std::map<librados::snap_t, uint8_t> SnapObjectStates;
  typedef std::map<librados::snap_t, std::map<uint64_t, uint64_t>> SnapObjectSizes;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  CephContext *m_cct;
  const SnapMap &m_snap_map;
  uint64_t m_dst_object_number;
  Context *m_on_finish;

  decltype(m_src_image_ctx->data_ctx) m_src_io_ctx;
  decltype(m_dst_image_ctx->data_ctx) m_dst_io_ctx;
  std::string m_src_oid;
  std::string m_dst_oid;

  ObjectOffsets m_src_object_offsets;
  librados::snap_set_t m_snap_set;
  int m_snap_ret;

  bool m_retry_missing_read = false;
  librados::snap_set_t m_retry_snap_set;

  SnapCopyOps m_snap_copy_ops;
  SnapObjectStates m_snap_object_states;
  SnapObjectSizes m_snap_object_sizes;

  std::map<uint64_t, librados::ObjectReadOperation> m_read_ops;

  void send_list_snaps();
  void handle_list_snaps(int r);

  void send_read_object();
  void handle_read_object(int r);

  void send_write_object();
  void handle_write_object(int r);

  void send_update_object_map();
  void handle_update_object_map(int r);

  Context *start_dst_op(RWLock &owner_lock);

  void init_offsets();
  void compute_diffs();
  void finish(int r);

  inline uint64_t src_to_dst_object_offset(uint64_t src_object_number,
                                           uint64_t src_object_offset) {
    return ((1 << m_src_image_ctx->order) * src_object_number +
            src_object_offset) % (1 << m_dst_image_ctx->order);
  }
  inline uint64_t dst_to_src_object_offset(uint64_t dst_object_offset) {
    return ((1 << m_dst_image_ctx->order) * m_dst_object_number +
            dst_object_offset) % (1 << m_src_image_ctx->order);
  }
};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H
