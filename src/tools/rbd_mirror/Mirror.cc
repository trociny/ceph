// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "Mirror.h"

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
using librbd::mirror_peer_t;

namespace rbd {
namespace mirror {

Mirror::Mirror(CephContext *cct) :
  m_cct(cct),
  m_lock("rbd::mirror::Mirror"),
  m_cluster(new librados::Rados())
{
}

void Mirror::handle_signal(int signum)
{
  m_stopping.set(1);
}

int Mirror::init()
{
  int r = m_cluster->init_with_context(m_cct);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    return r;
  }

  r = m_cluster->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    return r;
  }

  // TODO: make interval configurable
  m_pool_watcher.reset(new PoolWatcher(m_cluster, 30, m_lock, m_cond));
  m_pool_watcher->refresh_pools();
  m_image_watcher.reset(new ImageWatcher(m_cluster, 30, m_lock, m_cond));
  m_image_watcher->refresh_images();

  return r;
}

void Mirror::run()
{
  while (!m_stopping.read()) {
    Mutex::Locker l(m_lock);
    update_replayers(m_pool_watcher->get_peer_configs(),
		     m_image_watcher->get_images());
    m_cond.Wait(m_lock);
  }
}

void Mirror::update_replayers(map<peer_t, set<int64_t> > peer_configs,
			      map<int64_t, set<string> > images)
{
  assert(m_lock.is_locked());
  set<peer_t> peers;
  for (auto &kv : peer_configs) {
    peers.insert(kv.first);
  }
  refresh_peers(peers);
  for (auto &kv : m_replayers) {
    kv.second->set_sources(images);
  }
}

void Mirror::refresh_peers(set<peer_t> peers)
{
  assert(m_lock.is_locked());
  for (const peer_t &peer : peers) {
    if (m_replayers.find(peer) == m_replayers.end()) {
      unique_ptr<Replayer> replayer(new Replayer(m_cluster, peer));
      // TODO: make async, and retry connecting within replayer
      int r = replayer->start();
      if (r < 0) {
	continue;
      }
      m_replayers.insert(std::make_pair(peer, std::move(replayer)));
    }
  }

  // TODO: make async
  for (auto it = m_replayers.begin(); it != m_replayers.end();) {
    peer_t peer = it->first;
    if (peers.find(peer) == peers.end()) {
      m_replayers.erase(it++);
    } else {
      ++it;
    }
  }
}

} // namespace mirror
} // namespace rbd
