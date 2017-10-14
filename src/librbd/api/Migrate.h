// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_MIGRATE_H
#define CEPH_LIBRBD_API_MIGRATE_H

#include "include/int_types.h"
#include "include/rbd/librbd.hpp"
#include "cls/rbd/cls_rbd_types.h"

namespace librados {

class IoCtx;

}

namespace librbd {

class ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class Migrate {
public:
  static int migrate(ImageCtxT *image_ctx, librados::IoCtx& dst_io_ctx,
                     const char *dstname, ImageOptions& opts,
                     ProgressContext &prog_ctx);

private:
  CephContext* m_cct;
  ImageCtxT *m_image_ctx;
  librados::IoCtx m_src_io_ctx;
  librados::IoCtx &m_dst_io_ctx;
  std::string m_dstname;
  ImageOptions &m_image_options;
  ProgressContext &m_prog_ctx;

  Migrate(ImageCtxT *image_ctx, librados::IoCtx& dst_io_ctx,
          const std::string &dstname, ImageOptions& opts,
          ProgressContext &prog_ctx);

  int execute();
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Migrate<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_MIGRATE_H
