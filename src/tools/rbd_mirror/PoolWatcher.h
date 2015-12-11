// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_WATCHER_H
#define CEPH_RBD_MIRROR_POOL_WATCHER_H

#include <map>
#include <memory>
#include <set>

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "include/rbd/librbd.hpp"
#include "types.h"

namespace rbd {
namespace mirror {

/**
 * Tracks mirroring configuration for pools in a single
 * cluster. Simply polls for changes (pools added/removed, remote
 * differences) for now.
 */
class PoolWatcher {
public:
  PoolWatcher(RadosRef cluster, double interval_seconds,
	      Mutex &lock, Cond &cond);
  ~PoolWatcher();
  PoolWatcher(const PoolWatcher&) = delete;
  PoolWatcher& operator=(const PoolWatcher&) = delete;
  void refresh_pools();
  std::map<peer_t, std::set<int64_t> > get_peer_configs() const;

private:
  void read_configs(std::map<peer_t, std::set<int64_t> > *peer_configs);

  Mutex &m_lock;
  Cond &m_refresh_cond;

  RadosRef m_cluster;
  SafeTimer m_timer;
  double m_interval;
  std::map<peer_t, std::set<int64_t> > m_peer_configs;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_POOL_WATCHER_H
