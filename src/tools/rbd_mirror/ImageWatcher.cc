// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "ImageWatcher.h"

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
using librbd::Image;

namespace rbd {
namespace mirror {

namespace {

struct C_RefreshImages : public Context {
  C_RefreshImages(ImageWatcher *iw) :
    iw(iw) {}
  virtual void finish(int r) {
    iw->refresh_images();
  }
  ImageWatcher *iw;
};

} // anonymous namespace

ImageWatcher::ImageWatcher(RadosRef cluster, double interval_seconds,
			 Mutex &lock, Cond &cond) :
  m_lock(lock),
  m_refresh_cond(cond),
  m_cluster(cluster),
  m_timer(g_ceph_context, m_lock),
  m_interval(interval_seconds)
{
  m_timer.init();
}

ImageWatcher::~ImageWatcher()
{
  m_timer.shutdown();
}

map<int64_t, set<string> > ImageWatcher::get_images() const
{
  assert(m_lock.is_locked());
  return m_images;
}

void ImageWatcher::refresh_images()
{
  dout(20) << __func__ << dendl;
  map<int64_t, set<string> > images;
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
      dout(10) << "pool " << pool_name << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error accessing pool " << pool_name << cpp_strerror(r) << dendl;
      continue;
    }

    vector<string> pool_images;
    r = rbd.list(ioctx, pool_images);
    if (r < 0) {
      derr << "error listing images in pool " << pool_name << " : "
	   << cpp_strerror(r) << dendl;
      continue;
    }

    set<string> mirrored_images;
    for (string image_name : pool_images) {
      Image image;
      // TODO: index mirrored images so we don't need to read all headers
      r = rbd.open_read_only(ioctx, image, image_name.c_str(), nullptr);
      if (r < 0) {
	derr << "error opening image " << pool_name << "/" << image_name
	     << " : " << cpp_strerror(r) << dendl;
	continue;
      }
      uint64_t flags;
      r = image.get_flags(&flags);
      if (r < 0) {
	derr << "error reading flags for image " << pool_name << "/"
	     << image_name << " : " << cpp_strerror(r) << dendl;
	continue;
      }
      bool mirroring_enabled = (flags & RBD_FLAG_MIRRORING_ENABLED) != 0;
      dout(20) << "image " << pool_name << "/" << image_name
	       << ": mirroring "
	       << (mirroring_enabled ? "enabled" : "disabled") << dendl;
      if (mirroring_enabled) {
	mirrored_images.insert(image_name);
      }
    }
    if (!mirrored_images.empty()) {
      images[pool_id] = mirrored_images;
    }
  }

  Mutex::Locker l(m_lock);
  m_images = images;
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::refresh_images, this));
  m_timer.add_event_after(m_interval, ctx);
  // TODO: perhaps use a workqueue instead, once we get notifications
  // about config changes for existing pools
}

} // namespace mirror
} // namespace rbd
