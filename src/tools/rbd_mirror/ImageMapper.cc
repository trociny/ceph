// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "ImageMapper.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageMapper: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

template <typename I>
ImageMapper<I>::ImageMapper()
  : m_lock("rbd::mirror::ImageMapper") {
}

template <typename I>
void ImageMapper<I>::update(const ImageIds &image_ids,
              std::vector<std::string> *images_to_detach,
              std::vector<std::string> *images_to_attach) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  for (auto &it : m_images) {
    if (it.second != ATTACHED) {
      dout(20) << "global_image_id=" << it.first << ", state=" << it.second
	       << " (not attached)" << dendl;
      assert(it.second == ATTACHING || it.second == DETACHING);
    }

    if (image_ids.find(ImageId(it.first)) != image_ids.end()) {
      continue;
    }

    it.second = DETACHING;
    images_to_detach->push_back(it.first);
  }

  for (auto &image : image_ids) {
    auto it = m_images.find(image.global_id);
    if (it == m_images.end()) {
      m_images[image.global_id] = ATTACHING;
      images_to_attach->push_back(image.global_id);
    } else {
      if (it->second != ATTACHED) {
	dout(20) << "global_image_id=" << it->first << ", state=" << it->second
		 << " (not attached)" << dendl;
	assert(it->second == ATTACHING);
      }
    }
  }
}

template <typename I>
void ImageMapper<I>::attach(const std::string &global_image_id) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  Mutex::Locker locker(m_lock);

  auto it = m_images.find(global_image_id);

  assert(it != m_images.end());

  if (it->second != ATTACHING) {
    dout(20) << "global_image_id=" << it->first << ", state=" << it->second
	     << " (not attaching)" << dendl;
    assert(it->second == ATTACHED);
    return;
  }

  it->second = ATTACHED;
}

template <typename I>
void ImageMapper<I>::detach(const std::string &global_image_id) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  Mutex::Locker locker(m_lock);

  auto it = m_images.find(global_image_id);

  if (it == m_images.end()) {
    dout(20) << "global_image_id=" << global_image_id << ": already detached"
	     << dendl;
    return;
  }

  assert(it->second == DETACHING);

  m_images.erase(it);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageMapper<librbd::ImageCtx>;
