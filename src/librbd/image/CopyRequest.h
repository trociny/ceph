// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_COPY_REQUEST_H
#define CEPH_LIBRBD_IMAGE_COPY_REQUEST_H

#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include "common/Mutex.h"
#include "common/RefCountedObj.h"
#include <map>
#include <vector>

class Context;

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace image_copy {

template <typename> class ImageCopyRequest;
template <typename> class SnapshotCopyRequest;

}

namespace image {

template <typename ImageCtxT = ImageCtx>
class CopyRequest : public RefCountedObject {
public:
  static CopyRequest* create(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                             ContextWQ *work_queue, ProgressContext *prog_ctx,
                             Context *on_finish) {
    return new CopyRequest(src_image_ctx, dst_image_ctx, work_queue, prog_ctx,
                           on_finish);
  }

  CopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
              ContextWQ *work_queue, ProgressContext *prog_ctx,
              Context *on_finish);
  ~CopyRequest();

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * COPY_SNAPSHOTS
   *    |
   *    v
   * COPY_IMAGE . . . . . . . . . . . . . .
   *    |                                 .
   *    v                                 .
   * COPY_OBJECT_MAP (skip if object      .
   *    |             map disabled)       .
   *    v                                 .
   * REFRESH_OBJECT_MAP (skip if object   . (image copy canceled)
   *    |                map disabled)    .
   *    v                                 .
   * REMOVE_COPY_SNAPSHOT                 .
   *    |                                 .
   *    v                                 .
   * <finish> < . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  CephContext *m_cct;
  ContextWQ *m_work_queue;
  ProgressContext *m_prog_ctx;
  Context *m_on_finish;

  Mutex m_lock;
  bool m_canceled = false;
  SnapSeqs m_snap_seqs;
  librados::snap_t m_copy_snap_id;

  image_copy::SnapshotCopyRequest<ImageCtxT> *m_snapshot_copy_request = nullptr;
  image_copy::ImageCopyRequest<ImageCtxT> *m_image_copy_request = nullptr;
  decltype(ImageCtxT::object_map) m_object_map = nullptr;

  void send_copy_snapshots();
  void handle_copy_snapshots(int r);

  void send_copy_image();
  void handle_copy_image(int r);

  void send_copy_object_map();
  void handle_copy_object_map(int r);

  void send_refresh_object_map();
  void handle_refresh_object_map(int r);

  void send_remove_copy_snapshot();
  void handle_remove_copy_snapshot(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::CopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_COPY_REQUEST_H
