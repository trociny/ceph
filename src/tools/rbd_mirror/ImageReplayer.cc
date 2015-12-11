// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "ImageReplayer.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

ImageReplayer::ImageReplayer(RadosRef primary, RadosRef remote,
			     int64_t primary_pool_id, const string &image_name) :
  m_lock(stringify("rbd::mirror::ImageReplayer ") + stringify(primary_pool_id) + string(" ") + image_name),
  m_pool_id(primary_pool_id),
  m_image_name(image_name),
  m_primary(primary),
  m_remote(remote)
{
}

ImageReplayer::~ImageReplayer()
{
}

int ImageReplayer::start()
{
  int r = m_primary->ioctx_create2(m_pool_id, m_primary_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for pool " << m_pool_id
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }
  m_pool_name = m_primary_ioctx.get_pool_name();
  r = m_remote->ioctx_create(m_pool_name.c_str(), m_remote_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for remote pool " << m_pool_name
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void ImageReplayer::stop()
{
  m_remote_image.close();
  m_remote_ioctx.close();
  m_primary_ioctx.close();
}

} // namespace mirror
} // namespace rbd
