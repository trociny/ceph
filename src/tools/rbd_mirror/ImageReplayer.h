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

namespace journal {

class Journaler;
class ReplayHandler;

}

namespace librbd {

class ImageCtx;
class JournalReplay;

}

namespace rbd {
namespace mirror {

/**
 * Controls mirroring for a single remote cluster.
 */
class ImageReplayer {
public:
  ImageReplayer(RadosRef local, RadosRef remote, const string &local_pool_name,
		const string &remote_pool_name, const string &local_image_name,
		const string &remote_image_name = "");
  ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  int start();
  void stop();

  void handle_replay_ready();
  void handle_replay_complete(int r);

private:
  RadosRef m_local, m_remote;
  std::string m_local_pool_name, m_remote_pool_name;
  std::string m_local_image_name, m_remote_image_name;
  Mutex m_lock;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::JournalReplay *m_journal_replay;
  ::journal::Journaler *m_remote_journaler;
  ::journal::ReplayHandler *m_replay_handler;
  uint64_t last_commit_tid;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
