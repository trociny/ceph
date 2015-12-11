// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include <map>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rbd/librbd.hpp"
#include "types.h"

namespace rbd {
namespace mirror {

/**
 * Controls mirroring for a single remote cluster.
 */
class ImageReplayer {
public:
  ImageReplayer(RadosRef primary, RadosRef remote,
		int64_t primary_pool_id, const string &image_name);
  ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  int start();
  void stop();

private:
  Mutex m_lock;
  int64_t m_pool_id;
  std::string m_pool_name;
  std::string m_image_name;
  RadosRef m_primary, m_remote;
  librados::IoCtx m_primary_ioctx, m_remote_ioctx;
  librbd::Image m_remote_image;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
