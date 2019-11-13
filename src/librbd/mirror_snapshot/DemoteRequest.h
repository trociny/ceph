// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_DEMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_DEMOTE_REQUEST_H

#include "include/buffer.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror_snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class DemoteRequest {
public:
  static DemoteRequest *create(ImageCtxT *image_ctx, bool force,
                               Context *on_finish) {
    return new DemoteRequest(image_ctx, force, on_finish);
  }

  DemoteRequest(ImageCtxT *image_ctx, bool force, Context *on_finish)
    : m_image_ctx(image_ctx), m_force(force), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * REFRESH_IMAGE
   *    |
   *    v
   * CREATE_SNAPSHOT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  bool m_force;
  Context *m_on_finish;

  void refresh_image();
  void handle_refresh_image(int r);

  void create_snapshot();
  void handle_create_snapshot(int r);

  void finish(int r);

};

} // namespace mirror_snapshot
} // namespace librbd

extern template class librbd::mirror_snapshot::DemoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_DEMOTE_REQUEST_H
