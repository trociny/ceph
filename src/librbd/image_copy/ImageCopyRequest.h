// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_COPY_IMAGE_IMAGE_COPY_REQUEST_H
#define CEPH_LIBRBD_IMAGE_COPY_IMAGE_IMAGE_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/RefCountedObj.h"
#include "librbd/Types.h"
#include <map>
#include <vector>

class Context;

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace image_copy {

template <typename ImageCtxT = ImageCtx>
class ImageCopyRequest : public RefCountedObject {
public:
  static ImageCopyRequest* create(ImageCtxT *src_image_ctx,
                                  ImageCtxT *dst_image_ctx,
                                  const SnapSeqs &snap_seqs,
                                  ProgressContext *prog_ctx,
                                  Context *on_finish) {
    return new ImageCopyRequest(src_image_ctx, dst_image_ctx, snap_seqs,
                                prog_ctx, on_finish);
  }

  ImageCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                   const SnapSeqs &snap_seqs, ProgressContext *prog_ctx,
                   Context *on_finish);

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>   . . . . .
   *    |      .       .  (parallel execution of
   *    v      v       .   multiple objects at once)
   * COPY_OBJECT . . . .
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  const SnapSeqs &m_snap_seqs;
  ProgressContext *m_prog_ctx;
  Context *m_on_finish;

  CephContext *m_cct;
  Mutex m_lock;
  bool m_canceled = false;

  uint64_t m_object_no = 0;
  uint64_t m_end_object_no;
  uint64_t m_current_ops = 0;
  SnapMap m_snap_map;
  int m_ret_val = 0;

  void send_object_copies();
  void send_next_object_copy();
  void handle_object_copy(int r);

  void finish(int r);

  int compute_snap_map();
};

} // namespace image_copy
} // namespace librbd

extern template class librbd::image_copy::ImageCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_COPY_IMAGE_IMAGE_COPY_REQUEST_H
