// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "PoolWatcher.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

using std::list;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;

namespace rbd {
namespace mirror {

namespace {

struct C_RefreshPools : public Context {
  C_RefreshPools(PoolWatcher *pw) :
    pw(pw) {}
  virtual void finish(int r) {
    pw->refresh_pools();
  }
  PoolWatcher *pw;
};

} // anonymous namespace

PoolWatcher::PoolWatcher(RadosRef cluster, double interval_seconds,
			 Mutex &lock, Cond &cond) :
  m_lock(lock),
  m_refresh_cond(cond),
  m_cluster(cluster),
  m_timer(g_ceph_context, m_lock),
  m_interval(interval_seconds)
{
  m_timer.init();
}

PoolWatcher::~PoolWatcher()
{
  m_timer.shutdown();
}

map<peer_t, set<int64_t> > PoolWatcher::get_peer_configs() const
{
  assert(m_lock.is_locked());
  return m_peer_configs;
}

void PoolWatcher::refresh_pools()
{
  dout(20) << __func__ << dendl;
  map<peer_t, set<int64_t> > peer_configs;
  read_configs(&peer_configs);

  Mutex::Locker l(m_lock);
  m_peer_configs = peer_configs;
  m_timer.add_event_after(m_interval, new C_RefreshPools(this));
  // TODO: perhaps use a workqueue instead, once we get notifications
  // about config changes for existing pools
}

void PoolWatcher::read_configs(std::map<peer_t,
			       std::set<int64_t> > *peer_configs)
{
  list<pair<int64_t, string> > pools;
  int r = m_cluster->pool_list2(pools);
  if (r < 0) {
    derr << "error listing pools: " << cpp_strerror(r) << dendl;
    return;
  }

  librbd::RBD rbd;
  for (auto kv : pools) {
    int64_t pool_id = kv.first;
    string pool_name = kv.second;
    int64_t base_tier;
    r = m_cluster->pool_get_base_tier(pool_id, &base_tier);
    if (r == -ENOENT) {
      dout(10) << "pool " << pool_name << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error retrieving base tier for pool " << pool_name << dendl;
      continue;
    }
    if (pool_id != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    IoCtx ioctx;
    r = m_cluster->ioctx_create2(pool_id, ioctx);
    if (r == -ENOENT) {
      dout(10) << "pool " << pool_id << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error accessing pool " << pool_name << cpp_strerror(r) << dendl;
      continue;
    }

    bool enabled;
    r = rbd.mirror_is_enabled(ioctx, &enabled);
    if (r < 0) {
      derr << "could not tell whether mirroring was enabled for " << pool_name
	   << " : " << cpp_strerror(r) << dendl;
      continue;
    }
    if (!enabled) {
      dout(10) << "mirroring is disabled for pool " << pool_name << dendl;
      continue;
    }

    vector<librbd::mirror_peer_t> configs;
    r = rbd.mirror_peer_list(ioctx, &configs);
    if (r == -ENOENT)
      continue; // raced with disabling mirroring
    if (r < 0) {
      derr << "error reading mirroring config for pool " << pool_name
	   << cpp_strerror(r) << dendl;
      continue;
    }

    for (peer_t peer : configs) {
      (*peer_configs)[peer].insert(pool_id);
    }
  }
}

} // namespace mirror
} // namespace rbd
