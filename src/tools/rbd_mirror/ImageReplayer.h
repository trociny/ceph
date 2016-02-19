// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include <map>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "types.h"

namespace journal {

class Journaler;
class ReplayHandler;
class ReplayEntry;

}

namespace librbd {

class ImageCtx;

namespace journal {

template <typename> class Replay;

}

}

namespace rbd {
namespace mirror {

/**
 * Replays changes from a remote cluster for a single image.
 */
class ImageReplayer {
public:
  enum State {
    STATE_UNINITIALIZED,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_FLUSHING_REPLAY,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  struct BootstrapParams {
    std::string local_pool_name;
    std::string local_image_name;

    BootstrapParams() {}
    BootstrapParams(const std::string &local_pool_name,
		    const std::string local_image_name) :
      local_pool_name(local_pool_name),
      local_image_name(local_image_name) {}
  };

public:
  ImageReplayer(RadosRef local, RadosRef remote, int64_t remote_pool_id,
		const std::string &remote_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  int start(const BootstrapParams *bootstrap_params = nullptr);
  void stop();

  int flush();

  virtual void handle_replay_ready();
  virtual void handle_replay_process_ready(int r);
  virtual void handle_replay_complete(int r);

  virtual void handle_replay_committed(::journal::ReplayEntry* replay_entry, int r);

private:
  int get_registered_client_status(bool *registered);
  int register_client();
  int get_bootrstap_params(BootstrapParams *params);
  int bootstrap(const BootstrapParams *bootstrap_params);
  int create_local_image(const BootstrapParams &bootstrap_params);
  int get_image_id(librados::IoCtx &ioctx, const std::string &image_name,
		   std::string *image_id);
  int copy();

  void shut_down_journal_replay();

  friend std::ostream &operator<<(std::ostream &os,
				  const ImageReplayer &replayer);
private:
  RadosRef m_local, m_remote;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_local_cluster_id;
  std::string m_remote_image_id, m_local_image_id;
  std::string m_client_id;
  Mutex m_lock;
  State m_state;
  std::string m_local_pool_name, m_remote_pool_name;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::Replay<librbd::ImageCtx> *m_local_replay;
  ::journal::Journaler *m_remote_journaler;
  ::journal::ReplayHandler *m_replay_handler;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
