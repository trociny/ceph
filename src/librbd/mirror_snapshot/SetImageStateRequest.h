// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_SET_IMAGE_STATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_SET_IMAGE_STATE_REQUEST_H

#include "librbd/mirror_snapshot/Types.h"

#include <map>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror_snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class SetImageStateRequest {
public:
  static SetImageStateRequest *create(ImageCtxT *image_ctx, uint64_t snap_id,
                                      Context *on_finish) {
    return new SetImageStateRequest(image_ctx, snap_id, on_finish);
  }

  SetImageStateRequest(ImageCtxT *image_ctx, uint64_t snap_id,
                       Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_SNAP_LIMIT
   *    |
   *    v
   * GET_METADATA (repeat until
   *    |          all metadata read)
   *    v
   * WRITE_OBJECT (repeat for
   *    |          every object)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_snap_id;
  Context *m_on_finish;

  ImageState m_image_state;

  bufferlist m_bl;
  bufferlist m_state_bl;
  std::string m_last_metadata_key;

  const size_t m_object_size;
  size_t m_object_count = 0;

  void get_snap_limit();
  void handle_get_snap_limit(int r);

  void get_metadata();
  void handle_get_metadata(int r);

  void write_object();
  void handle_write_object(int r);

  void finish(int r);
};

} // namespace mirror_snapshot
} // namespace librbd

extern template class librbd::mirror_snapshot::SetImageStateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_SET_IMAGE_STATE_REQUEST_H
