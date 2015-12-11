// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_WATCHER_H
#define CEPH_RBD_MIRROR_IMAGE_WATCHER_H

#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "include/rbd/librbd.hpp"
#include "types.h"

namespace rbd {
namespace mirror {

struct ImageChange {
  std::map<int64_t, std::set<peer_t> > new_pool_configs;
  std::map<int64_t, std::set<peer_t> > added_peers;
  std::map<int64_t, std::set<peer_t> > removed_peers;
};

/**
 * Keeps track of images that have mirroring enabled.
 */
class ImageWatcher {
public:
  ImageWatcher(RadosRef cluster, double interval_seconds,
	       Mutex &lock, Cond &cond);
  ~ImageWatcher();
  ImageWatcher(const ImageWatcher&) = delete;
  ImageWatcher& operator=(const ImageWatcher&) = delete;
  std::map<int64_t, std::set<std::string> > get_images() const;
  void refresh_images();

private:
  Mutex &m_lock;
  Cond &m_refresh_cond;

  RadosRef m_cluster;
  SafeTimer m_timer;
  double m_interval;
  std::map<int64_t, std::set<std::string> > m_images;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_WATCHER_H
