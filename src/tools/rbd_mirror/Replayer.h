// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_REPLAYER_H
#define CEPH_RBD_MIRROR_REPLAYER_H

#include <map>
#include <set>
#include <string>

#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rbd/librbd.hpp"
#include "types.h"
#include "ImageReplayer.h"

namespace rbd {
namespace mirror {

/**
 * Controls mirroring for a single remote cluster.
 */
class Replayer {
public:
  Replayer(RadosRef primary_cluster, const peer_t &peer);
  ~Replayer();
  Replayer(const Replayer&) = delete;
  Replayer& operator=(const Replayer&) = delete;

  int start();
  void stop();
  void set_sources(const std::map<int64_t, std::set<std::string> > &images);

private:
  Mutex m_lock;
  peer_t m_peer;
  RadosRef m_primary, m_remote;
  // index by pool so it's easy to tell what is affected
  // when a pool's configuration changes
  std::map<int64_t, std::map<std::string,
			     std::unique_ptr<ImageReplayer> > > m_images;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_REPLAYER_H
