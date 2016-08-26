// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_MIRROR_ENABLE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_MIRROR_ENABLE_REQUEST_H

#include "include/buffer.h"
#include "cls/rbd/cls_rbd_types.h"
#include <map>
#include <string>

class Context;

namespace librados { class IoCtx; }

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class MirrorEnableRequest {
public:
  static MirrorEnableRequest *create(librados::IoCtx *io_ctx,
				     const std::string &image_id,
				     Context *on_finish) {
    return new MirrorEnableRequest(io_ctx, image_id, on_finish);
  }
  static MirrorEnableRequest *create(ImageCtxT *image_ctx, Context *on_finish) {
    return new MirrorEnableRequest(image_ctx, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_IMAGE (skip if not  * * *
   *    |        required)        *
   *    v                         *
   * GET_TAG_OWNER  * * * * * * * *
   *    |                         *
   *    v                         *
   * GET_MIRROR_IMAGE * * * * * * *
   *    |                         * (on error)
   *    v                         *
   * SET_MIRROR_IMAGE * * * * * * *
   *    |                         *
   *    v                         *
   * NOTIFY_MIRRORING_WATCHER     *
   *    |                         *
   *    v                         *
   * CLOSE_IMAGE (skip if not < * *
   *    |         required)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  MirrorEnableRequest(librados::IoCtx *io_ctx, const std::string &image_id,
		      Context *on_finish);
  MirrorEnableRequest(ImageCtxT *image_ctx, Context *on_finish);

  librados::IoCtx *m_io_ctx = nullptr;
  std::string m_image_id;
  ImageCtxT *m_image_ctx = nullptr;
  Context *m_on_finish;

  bool m_close_image_on_finish = false;
  bool m_is_primary = false;
  bufferlist m_out_bl;
  cls::rbd::MirrorImage m_mirror_image;
  int m_error_result = 0;

  void send_open_image();
  Context *handle_open_image(int *result);

  void send_get_tag_owner();
  Context *handle_get_tag_owner(int *result);

  void send_get_mirror_image();
  Context *handle_get_mirror_image(int *result);

  void send_set_mirror_image();
  Context *handle_set_mirror_image(int *result);

  void send_notify_mirroring_watcher();
  Context *handle_notify_mirroring_watcher(int *result);

  void send_close_image();
  Context *handle_close_image(int *result);

  Context *handle_finish(int *result);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::MirrorEnableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_MIRROR_ENABLE_REQUEST_H
