// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_REMOVE_OBJECT_MAP_REQUEST_H
#define CEPH_LIBRBD_IMAGE_REMOVE_OBJECT_MAP_REQUEST_H

#include "include/buffer.h"
#include "common/Mutex.h"
#include <map>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class RemoveObjectMapRequest {
public:
  static RemoveObjectMapRequest *create(ImageCtxT *image_ctx, Context *on_finish) {
    return new RemoveObjectMapRequest(image_ctx, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |          .  .  .
   *    v          v     .
   * REMOVE_OBJECT_MAP   . (for every snapshot)
   *    |          .     .
   *    v          .  .  .
   * <finis>
   *
   * @endverbatim
   */

  RemoveObjectMapRequest(ImageCtxT *image_ctx, Context *on_finish);

  ImageCtxT *m_image_ctx;
  Context *m_on_finish;

  int m_error_result = 0;
  int m_ref_counter = 0;
  mutable Mutex m_lock;

  void send_remove_object_map();
  Context *handle_remove_object_map(int *result);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::RemoveObjectMapRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_REMOVE_OBJECT_MAP_REQUEST_H
