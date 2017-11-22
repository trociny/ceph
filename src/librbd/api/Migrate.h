// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_MIGRATE_H
#define CEPH_LIBRBD_API_MIGRATE_H

#include "include/int_types.h"
#include "include/rbd/librbd.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <vector>

namespace librados {

class IoCtx;

}

namespace librbd {

class ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class Migrate {
public:
  static int migrate(librados::IoCtx& io_ctx, const std::string &image_name,
                     librados::IoCtx& dest_io_ctx,
                     const std::string &dest_image_name, ImageOptions& opts,
                     ProgressContext &prog_ctx);
  static int abort(librados::IoCtx& io_ctx, const std::string &image_name,
                   ProgressContext &prog_ctx);

private:
  CephContext* m_cct;
  ImageCtxT *m_src_image_ctx;
  librados::IoCtx m_src_io_ctx;
  librados::IoCtx &m_dst_io_ctx;
  bool m_src_old_format;
  std::string m_src_image_name;
  std::string m_src_image_id;
  std::string m_src_header_oid;
  std::string m_dst_image_name;
  ImageOptions &m_image_options;
  ProgressContext &m_prog_ctx;

  cls::rbd::MirrorImage m_src_mirror_image;
  std::string m_dst_image_id;
  cls::rbd::MigrationSpec m_src_migration_spec;
  std::vector<librbd::snap_info_t> m_snaps;

  Migrate(ImageCtxT *image_ctx, librados::IoCtx& dest_io_ctx,
          const std::string &dest_image_name, ImageOptions& opts,
          ProgressContext &prog_ctx);

  int migrate();

  int list_snaps();
  int disable_mirroring();
  int set_migration();
  int create_dst_image(ImageCtxT **dst_image_ctx);
  int enable_mirroring();
  int unset_migration(ImageCtxT *image_ctx);
  int remove_src_image();

  int v1_set_migration();
  int v2_set_migration();
  int v2_unlink_image();
  int v2_relink_image();

  int abort(const std::string &dst_image_id, bool mirroring);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Migrate<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_MIGRATE_H
