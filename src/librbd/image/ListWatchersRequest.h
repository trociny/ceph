// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_LIST_WATCHERS_REQUEST_H
#define CEPH_LIBRBD_IMAGE_LIST_WATCHERS_REQUEST_H

#include "include/rados/rados_types.hpp"
#include "librbd/Types.h"

#include <list>

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template<typename ImageCtxT = ImageCtx>
class ListWatchersRequest {
public:
  static ListWatchersRequest *create(ImageCtxT &image_ctx,
                                     std::list<WatcherInfo> *watchers,
                                     Context *on_finish) {
    return new ListWatchersRequest(image_ctx, watchers, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_IMAGE_WATCHERS
   *    |
   *    v
   * GET_MIRROR_IMAGE
   *    |
   *    v
   * LIST_MIRROR_WATCHERS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ListWatchersRequest(ImageCtxT &image_ctx, std::list<WatcherInfo> *watchers,
                      Context *on_finish);

  ImageCtxT& m_image_ctx;
  std::list<WatcherInfo> *m_watchers;
  Context *m_on_finish;

  CephContext *m_cct;
  int m_ret_val;
  bufferlist m_out_bl;
  std::list<obj_watch_t> m_object_watchers;
  std::list<obj_watch_t> m_mirror_watchers;

  void list_image_watchers();
  void handle_list_image_watchers(int r);

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void list_mirror_watchers();
  void handle_list_mirror_watchers(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::ListWatchersRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_LIST_WATCHERS_REQUEST_H
