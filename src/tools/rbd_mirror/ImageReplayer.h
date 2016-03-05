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
#include "cls/journal/cls_journal_types.h"
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

class ImageReplayerAdminSocketHook;

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

    bool empty() const {
      return local_pool_name.empty() && local_image_name.empty();
    }
  };

public:
  ImageReplayer(RadosRef local, RadosRef remote, const std::string &client_id,
		int64_t remote_pool_id, const std::string &remote_image_id,
		ContextWQ *work_queue);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  State get_state() const { return m_state; }
  bool is_stopped() const { return m_state == STATE_UNINITIALIZED ||
                                   m_state == STATE_STOPPED; }
  bool is_running() const { return !is_stopped() && m_state != STATE_STOPPING; }

  void start(Context *on_finish = nullptr,
	     const BootstrapParams *bootstrap_params = nullptr);
  void stop(Context *on_finish = nullptr);
  int flush();

  int bootstrap(const BootstrapParams &bootstrap_params);

  virtual void handle_replay_ready();
  virtual void handle_replay_process_ready(int r);
  virtual void handle_replay_complete(int r);

  virtual void handle_replay_committed(::journal::ReplayEntry* replay_entry, int r);

protected:
  /**
   * @verbatim
   *                   (error)
   * <uninitialized> <------------------- FAIL
   *    |                                  ^
   *    v                                  |
   * <starting>                            |
   *    |                                  |
   *    v                         (error)  |
   * GET_REGISTERED_CLIENT_STATUS -------->|
   *    |                                  |
   *    v                           (error)|
   * BOOTSTRAP (skip if not needed) ------>|
   *    |                                  |
   *    v                  (error)         |
   * REMOTE_JOURNALER_INIT --------------->|
   *    |                                  |
   *    v             (error)              |
   * LOCAL_IMAGE_OPEN -------------------->|
   *    |                                  |
   *    v             (error)              |
   * LOCAL_IMAGE_LOCK -------------------->|
   *    |                                  |
   *    v                         (error)  |
   * WAIT_FOR_LOCAL_JOURNAL_READY -------->/
   *    |
   *    v
   * <replaying>
   *    |
   *    v
   * <stopping>
   *    |
   *    v
   * JOURNAL_REPLAY_SHUT_DOWN
   *    |
   *    v
   * LOCAL_IMAGE_CLOSE
   *    |
   *    v
   * LOCAL_IMAGE_DELETE
   *    |
   *    v
   * <stopped>
   *
   * @endverbatim
   */

  virtual void on_start_get_registered_client_status_start(
    const BootstrapParams *bootstrap_params);
  virtual void on_start_get_registered_client_status_finish(int r,
    const std::set<cls::journal::Client> &registered_clients,
    const BootstrapParams &bootstrap_params);
  virtual void on_start_bootstrap_start(const BootstrapParams &params);
  virtual void on_start_bootstrap_finish(int r);
  virtual void on_start_remote_journaler_init_start();
  virtual void on_start_remote_journaler_init_finish(int r);
  virtual void on_start_local_image_open_start();
  virtual void on_start_local_image_open_finish(int r);
  virtual void on_start_local_image_lock_start();
  virtual void on_start_local_image_lock_finish(int r);
  virtual void on_start_wait_for_local_journal_ready_start();
  virtual void on_start_wait_for_local_journal_ready_finish(int r);
  virtual void on_start_fail_start(int r);
  virtual void on_start_fail_finish(int r);
  virtual void on_start_finish(int r);

  virtual void on_stop_journal_replay_shut_down_start();
  virtual void on_stop_journal_replay_shut_down_finish(int r);
  virtual void on_stop_local_image_close_start();
  virtual void on_stop_local_image_close_finish(int r);
  virtual void on_stop_local_image_delete_start();
  virtual void on_stop_local_image_delete_finish(int r);

  void close_local_image(Context *on_finish); // for tests

private:
  int get_bootrstap_params(BootstrapParams *params);
  int register_client();
  int create_local_image(const BootstrapParams &bootstrap_params);
  int get_image_id(librados::IoCtx &ioctx, const std::string &image_name,
		   std::string *image_id);
  int copy();

  void shut_down_journal_replay(bool cancel_ops);

  friend std::ostream &operator<<(std::ostream &os,
				  const ImageReplayer &replayer);
private:
  RadosRef m_local, m_remote;
  std::string m_client_id;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_remote_image_id, m_local_image_id;
  std::string m_snap_name;
  ContextWQ *m_work_queue;
  Mutex m_lock;
  State m_state;
  std::string m_local_pool_name, m_remote_pool_name;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::Replay<librbd::ImageCtx> *m_local_replay;
  ::journal::Journaler *m_remote_journaler;
  ::journal::ReplayHandler *m_replay_handler;
  Context *m_on_finish;
  ImageReplayerAdminSocketHook *m_asok_hook;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
