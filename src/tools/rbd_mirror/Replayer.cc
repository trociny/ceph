// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "Replayer.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

Replayer::Replayer(RadosRef primary_cluster, const peer_t &peer) :
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_primary(primary_cluster)
{
}

Replayer::~Replayer()
{
}

int Replayer::start()
{
  int r = m_remote->init2(m_peer.client_name.c_str(),
			  m_peer.cluster_name.c_str(), 0);
  if (r < 0) {
    derr << "error initializing remote cluster for " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote->conf_read_file(nullptr);
  if (r < 0) {
    derr << "could not read ceph conf for " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote->connect();
  if (r < 0) {
    derr << "error connecting to remote cluster " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  string cluster_uuid;
  r = m_remote->cluster_fsid(&cluster_uuid);
  if (r < 0) {
    derr << "error reading cluster uuid from remote cluster " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  if (cluster_uuid != m_peer.cluster_uuid) {
    derr << "configured cluster uuid does not match actual cluster uuid. "
	 << "expected: " << m_peer.cluster_uuid
	 << " observed: " << cluster_uuid << dendl;
    return -EINVAL;
  }

  return 0;
}

void Replayer::stop()
{
  m_remote->shutdown();
}

void Replayer::set_sources(const map<int64_t, set<string> > &images)
{
  Mutex::Locker l(m_lock);
  // TODO: make stopping and starting ImageReplayers async
  for (auto it = m_images.begin(); it != m_images.end();) {
    int64_t pool_id = it->first;
    auto &pool_images = it->second;
    if (images.find(pool_id) == images.end()) {
      m_images.erase(it++);
      continue;
    }
    for (auto images_it = pool_images.begin();
	 images_it != pool_images.end();) {
      //      const set<string>::const_iterator find_it =
      //	images.at(pool_id).find(images_it->first);
      if (images.at(pool_id).find(images_it->first) ==
	  images.at(pool_id).end()) {
	pool_images.erase(images_it++);
      } else {
	++images_it;
      }
    }
    ++it;
  }

  for (const auto &kv : images) {
    int64_t pool_id = kv.first;
    // create entry for pool if it doesn't exist
    auto &pool_replayers = m_images[pool_id];
    for (const auto &image_name : kv.second) {
      if (pool_replayers.find(image_name) == pool_replayers.end()) {
	unique_ptr<ImageReplayer> image_replayer(new ImageReplayer(m_primary,
								   m_remote,
								   "pool_id",
								   "XXXGM",
								   image_name));
	int r = image_replayer->start();
	if (r < 0) {
	  continue;
	}
	pool_replayers.insert(std::make_pair(image_name, std::move(image_replayer)));
      }
    }
  }
}

} // namespace mirror
} // namespace rbd
