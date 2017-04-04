// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_INSTANCE_SYNC_THROTTLER_H
#define RBD_MIRROR_INSTANCE_SYNC_THROTTLER_H

#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "common/Mutex.h"
#include "common/config_obs.h"

class Context;

namespace ceph { class Formatter; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;
template <typename> struct LeaderWatcher;

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceSyncThrottler : public md_config_obs_t {
public:
  static InstanceSyncThrottler *create(Threads<ImageCtxT> *threads,
                                       LeaderWatcher<ImageCtxT> *watcher) {
    return new InstanceSyncThrottler(threads, watcher);
  }
  void destroy() {
    delete this;
  }

  InstanceSyncThrottler(Threads<ImageCtxT> *threads,
                        LeaderWatcher<ImageCtxT> *leader_watcher);
  ~InstanceSyncThrottler() override;

  void set_max_concurrent_syncs(uint32_t max);
  void start_op(const std::string &id, Context *on_start);
  bool cancel_op(const std::string &id);
  void finish_op(const std::string &id);

  void handle_sync_request(const std::string &instance_id,
                           const std::string &request_id);
  void handle_sync_request_ack(const std::string &instance_id,
                               const std::string &request_id);
  void handle_sync_start(const std::string &instance_id,
                         const std::string &request_id);
  void handle_sync_complete(const std::string &instance_id,
                            const std::string &request_id);

  void handle_instance_removed(const std::string &instance_id);
  void handle_leader_acquired();
  void handle_leader_released();

  void print_status(Formatter *f, std::stringstream *ss);

private:
  typedef std::pair<std::string, std::string> Id;
  typedef std::pair<Context *, Context *> Op;

  Threads<ImageCtxT> *m_threads;
  LeaderWatcher<ImageCtxT> *m_leader_watcher;

  Mutex m_lock;
  std::string m_instance_id;
  uint32_t m_max_concurrent_syncs;
  bool m_leader = false;
  std::map<std::string, Op> m_proxied_ops;
  std::map<Id, Op> m_remote_ops;
  std::list<std::pair<Id, Context *>> m_queue;
  std::set<Id> m_inflight_ops;

  const char **get_tracked_conf_keys() const override;
  void handle_conf_change(const struct md_config_t *conf,
                          const std::set<std::string> &changed) override;

  void start_op(const std::string &instance_id, const std::string &id,
                Context *on_start);
  bool cancel_op(const std::string &instance_id, const std::string &id);
  void finish_op(const std::string &instance_id, const std::string &id);

  void schedule_proxied_op_sync_request(const std::string &id, int after);
  void schedule_remote_op_sync_request_ack(const Id &id, int after);
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::InstanceSyncThrottler<librbd::ImageCtx>;

#endif // RBD_MIRROR_INSTANCE_SYNC_THROTTLER_H
