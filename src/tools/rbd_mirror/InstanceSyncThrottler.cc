// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "InstanceSyncThrottler.h"
#include "common/Formatter.h"
#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "LeaderWatcher.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::InstanceSyncThrottler:: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace mirror {

template <typename I>
InstanceSyncThrottler<I>::InstanceSyncThrottler(
    Threads<I> *threads, LeaderWatcher<I> *leader_watcher)
  : m_threads(threads), m_leader_watcher(leader_watcher),
    m_lock(librbd::util::unique_lock_name("rbd::mirror::InstanceSyncThrottler",
                                          this)),
    m_max_concurrent_syncs(
        g_ceph_context->_conf->rbd_mirror_concurrent_image_syncs) {
  g_ceph_context->_conf->add_observer(this);
  Mutex::Locker locker(m_lock);
  if (m_leader_watcher != nullptr) {
    m_instance_id = m_leader_watcher->get_instance_id();
  } else {
    m_leader = true;
  }
  dout(20) << "leader_watcher=" << m_leader_watcher << ", "
           << "instance_id=" << m_instance_id << ", "
           << "max_concurrent_syncs=" << m_max_concurrent_syncs << ", "
           << "leader=" << m_leader << dendl;
}

template <typename I>
InstanceSyncThrottler<I>::~InstanceSyncThrottler() {
  g_ceph_context->_conf->remove_observer(this);

  Mutex::Locker locker(m_lock);
  assert(!m_leader || m_leader_watcher == nullptr);
  assert(m_proxied_ops.empty());
  assert(m_remote_ops.empty());
  assert(m_inflight_ops.empty());
  assert(m_queue.empty());
}

template <typename I>
void InstanceSyncThrottler<I>::start_op(const std::string &id,
                                        Context *on_start) {
  dout(20) << "id=" << id << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (!m_leader) {
    assert(m_proxied_ops.count(id) == 0);
    m_proxied_ops[id] = Op(on_start, nullptr);
    schedule_proxied_op_sync_request(id, 0);
    return;
  }

  start_op(m_instance_id, id, on_start);
}

template <typename I>
bool InstanceSyncThrottler<I>::cancel_op(const std::string &id) {
  dout(20) << "id=" << id << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  auto it = m_proxied_ops.find(id);
  if (it != m_proxied_ops.end()) {
    auto &op = it->second;
    if (op.first != nullptr) {
      assert(op.second != nullptr);
      bool canceled = m_threads->timer->cancel_event(op.second);
      assert(canceled);

      m_threads->work_queue->queue(op.first, -ECANCELED);
      m_leader_watcher->notify_sync_complete(m_instance_id, id);
      m_proxied_ops.erase(it);
      return true;
    } else {
      return false;
    }
  }

  return cancel_op(m_instance_id, id);
}

template <typename I>
void InstanceSyncThrottler<I>::finish_op(const std::string &id) {
  dout(20) << "id=" << id << dendl;

  Mutex::Locker locker(m_lock);

  auto it = m_proxied_ops.find(id);
  if (it != m_proxied_ops.end()) {
    auto &op = it->second;
    assert(op.first == nullptr);
    assert(op.second == nullptr);
    m_proxied_ops.erase(it);
    m_leader_watcher->notify_sync_complete(m_instance_id, id);
    return;
  }

  finish_op(m_instance_id, id);
}

template <typename I>
void InstanceSyncThrottler<I>::handle_sync_request(
    const std::string &instance_id, const std::string &request_id) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  Mutex::Locker locker(m_lock);
  if (!m_leader) {
    return;
  }

  Id id(instance_id, request_id);
  auto it = m_remote_ops.find(id);
  if (it != m_remote_ops.end()) {
    dout(20) << "notification duplicate" << dendl;
    return;
  }

  auto on_start = new FunctionContext(
      [this, id](int r) {
        Mutex::Locker locker(m_lock);
        if (r == -ECANCELED) {
          auto result = m_remote_ops.erase(id);
          assert(result > 0);
          return;
        }
        assert(r == 0);
        m_remote_ops[id] = Op(nullptr, nullptr);
        schedule_remote_op_sync_request_ack(id, 0);
      });
  m_remote_ops[id] = Op(on_start, nullptr);

  start_op(instance_id, request_id, on_start);
}

template <typename I>
void InstanceSyncThrottler<I>::handle_sync_request_ack(
    const std::string &instance_id, const std::string &request_id) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (instance_id != m_instance_id) {
    return;
  }

  m_leader_watcher->notify_sync_start(m_instance_id, request_id);

  auto it = m_proxied_ops.find(request_id);
  if (it == m_proxied_ops.end()) {
    dout(20) << "op not found, notification duplicate" << dendl;
    m_leader_watcher->notify_sync_complete(m_instance_id, request_id);
    return;
  }

  auto &op = it->second;

  if (op.first == nullptr) {
    dout(20) << "op already started, notification duplicate" << dendl;
    return;
  }

  // stop sending sync_request notifications
  assert(op.second != nullptr);
  bool canceled = m_threads->timer->cancel_event(op.second);
  assert(canceled);
  op.second = nullptr;

  m_threads->work_queue->queue(op.first, 0);
  op.first = nullptr;
}

template <typename I>
void InstanceSyncThrottler<I>::handle_sync_start(
    const std::string &instance_id, const std::string &request_id) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  // XXXMG: actually nothing to do
}

template <typename I>
void InstanceSyncThrottler<I>::handle_sync_complete(
    const std::string &instance_id, const std::string &request_id) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  Id id(instance_id, request_id);
  auto it = m_remote_ops.find(id);
  if (it == m_remote_ops.end()) {
    dout(20) << "notification duplicate" << dendl;
    return;
  }

  auto &op = it->second;

  if (op.first != nullptr) {
    assert(op.second == nullptr);
    dout(20) << "complete for not acked op (canceled)" << dendl;
    m_threads->work_queue->queue(op.first, -ECANCELED);
    op.first = nullptr;
    return;
  }

  // stop sending sync_request_ack notifications
  assert(op.second != nullptr);
  bool canceled = m_threads->timer->cancel_event(op.second);
  assert(canceled);

  m_remote_ops.erase(it);
  finish_op(instance_id, request_id);
}

template <typename I>
void InstanceSyncThrottler<I>::handle_instance_removed(
    const std::string &instance_id) {
  dout(20) << "instance_id=" << instance_id << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  for (auto it = m_remote_ops.begin(); it != m_remote_ops.end(); ) {
    auto &id = it->first;
    if (id.first == instance_id) {
      auto &op = it->second;
      if (op.second != nullptr) {
        bool canceled = m_threads->timer->cancel_event(op.second);
        assert(canceled);
        op.second = nullptr;
      }
      cancel_op(id.first, id.second);
      it = m_remote_ops.erase(it);
    } else {
      it++;
    }
  }
}

template <typename I>
void InstanceSyncThrottler<I>::handle_leader_acquired() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_leader = true;

  for (auto it = m_proxied_ops.begin(); it != m_proxied_ops.end(); ) {
    auto &id = it->first;
    auto &op = it->second;
    if (op.first != nullptr) {
      start_op(m_instance_id, id, op.first);
    } else {
      assert(op.second != nullptr);
      bool canceled = m_threads->timer->cancel_event(op.second);
      assert(canceled);
    }
    it = m_proxied_ops.erase(it);
  }
}

template <typename I>
void InstanceSyncThrottler<I>::handle_leader_released() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_leader = false;

  for (auto it = m_remote_ops.begin(); it != m_remote_ops.end(); ) {
    auto &id = it->first;
    auto &op = it->second;
    if (op.second != nullptr) {
      bool canceled = m_threads->timer->cancel_event(op.second);
      assert(canceled);
      op.second = nullptr;
    }
    if (cancel_op(id.first, id.second)) {
      it++;
    } else {
      it = m_remote_ops.erase(it);
    }
  }
}

template <typename I>
void InstanceSyncThrottler<I>::schedule_proxied_op_sync_request(
    const std::string &id, int after) {
  dout(20) << "id=" << id << ", after=" << after << dendl;

  assert(m_threads->timer_lock.is_locked());
  assert(m_lock.is_locked());

  assert(m_proxied_ops.count(id) > 0);
  assert(m_proxied_ops[id].second == nullptr);

  auto timer_task = new FunctionContext(
      [this, id](int r) {
        m_leader_watcher->notify_sync_request(m_instance_id, id);

        assert(m_threads->timer_lock.is_locked());
        Mutex::Locker locker(m_lock);
        assert(m_proxied_ops.count(id) > 0);
        m_proxied_ops[id].second = nullptr;
        schedule_proxied_op_sync_request(id, 10);
      });

  m_proxied_ops[id].second = timer_task;

  dout(20) << "scheduling sync_request after " << after << " sec (task "
           << timer_task << ")" << dendl;
  m_threads->timer->add_event_after(after, timer_task);
}

template <typename I>
void InstanceSyncThrottler<I>::schedule_remote_op_sync_request_ack(
  const Id &id, int after) {
  dout(20) << "id=" << id << ", after=" << after << dendl;

  assert(m_threads->timer_lock.is_locked());
  assert(m_lock.is_locked());

  assert(m_remote_ops.count(id) > 0);
  assert(m_remote_ops[id].second == nullptr);

  auto timer_task = new FunctionContext(
      [this, id](int r) {
        m_leader_watcher->notify_sync_request_ack(id.first, id.second);

        assert(m_threads->timer_lock.is_locked());
        Mutex::Locker locker(m_lock);
        assert(m_remote_ops.count(id) > 0);
        m_remote_ops[id].second = nullptr;
        schedule_remote_op_sync_request_ack(id, 10);
      });

  m_remote_ops[id].second = timer_task;

  dout(20) << "scheduling sync_request_ack after " << after << " sec (task "
           << timer_task << ")" << dendl;
  m_threads->timer->add_event_after(after, timer_task);
}

template <typename I>
void InstanceSyncThrottler<I>::start_op(const std::string &instance_id,
                                        const std::string &request_id,
                                        Context *on_start) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  assert(m_lock.is_locked());

  Id id(instance_id, request_id);

  if (m_max_concurrent_syncs == 0 ||
      m_inflight_ops.size() < m_max_concurrent_syncs) {
    assert(m_queue.empty());
    assert(m_inflight_ops.count(id) == 0);
    m_inflight_ops.insert(id);
    dout(20) << "ready to start sync for " << id << " ["
             << m_inflight_ops.size() << "/" << m_max_concurrent_syncs << "]"
             << dendl;
    m_threads->work_queue->queue(on_start, 0);
    return;
  }

  m_queue.push_back(std::make_pair(id, on_start));
  dout(20) << "image sync for " << id << " has been queued" << dendl;
}

template <typename I>
bool InstanceSyncThrottler<I>::cancel_op(const std::string &instance_id,
                                         const std::string &request_id) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  assert(m_lock.is_locked());

  Id id(instance_id, request_id);

  for (auto it = m_queue.begin(); it != m_queue.end(); ++it) {
    if (it->first == id) {
      dout(20) << "canceled queued sync for " << id << dendl;
      auto on_start = it->second;
      m_queue.erase(it);
      m_threads->work_queue->queue(on_start, -ECANCELED);
      return true;
    }
  }

  return false;
}

template <typename I>
void InstanceSyncThrottler<I>::finish_op(const std::string &instance_id,
                                         const std::string &request_id) {
  dout(20) << "instance_id=" << instance_id << ", request_id=" << request_id
           << dendl;

  assert(m_lock.is_locked());

  Id id(instance_id, request_id);

  auto result = m_inflight_ops.erase(id);
  assert(result > 0);

  if (m_inflight_ops.size() < m_max_concurrent_syncs && !m_queue.empty()) {
    auto pair = m_queue.front();
    m_inflight_ops.insert(pair.first);
    dout(20) << "ready to start sync for " << pair.first << " ["
             << m_inflight_ops.size() << "/" << m_max_concurrent_syncs << "]"
             << dendl;
    auto on_start = pair.second;
    m_threads->work_queue->queue(on_start, 0);
    m_queue.pop_front();
  }
}

template <typename I>
void InstanceSyncThrottler<I>::set_max_concurrent_syncs(uint32_t max) {
  dout(20) << "max=" << max << dendl;

  std::list<Context *> ops;
  {
    Mutex::Locker locker(m_lock);
    m_max_concurrent_syncs = max;

    // Start waiting ops in the case of available free slots
    while ((m_max_concurrent_syncs == 0 ||
            m_inflight_ops.size() < m_max_concurrent_syncs) &&
           !m_queue.empty()) {
      auto pair = m_queue.front();
      m_inflight_ops.insert(pair.first);
      dout(20) << "ready to start sync for " << pair.first << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_syncs << "]"
               << dendl;
      ops.push_back(pair.second);
      m_queue.pop_front();
    }
  }

  for (const auto& ctx : ops) {
    ctx->complete(0);
  }
}

template <typename I>
void InstanceSyncThrottler<I>::print_status(Formatter *f,
                                            std::stringstream *ss) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  if (f) {
    f->dump_int("max_parallel_syncs", m_max_concurrent_syncs);
    f->dump_int("running_syncs", m_inflight_ops.size());
    f->dump_int("waiting_syncs", m_queue.size());
    f->flush(*ss);
  } else {
    *ss << "[ ";
    *ss << "max_parallel_syncs=" << m_max_concurrent_syncs << ", ";
    *ss << "running_syncs=" << m_inflight_ops.size() << ", ";
    *ss << "waiting_syncs=" << m_queue.size() << " ]";
  }
}

template <typename I>
const char** InstanceSyncThrottler<I>::get_tracked_conf_keys() const {
  static const char* KEYS[] = {
    "rbd_mirror_concurrent_image_syncs",
    NULL
  };
  return KEYS;
}

template <typename I>
void InstanceSyncThrottler<I>::handle_conf_change(const struct md_config_t *conf,
                                      const set<string> &changed) {
  if (changed.count("rbd_mirror_concurrent_image_syncs")) {
    set_max_concurrent_syncs(conf->rbd_mirror_concurrent_image_syncs);
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::InstanceSyncThrottler<librbd::ImageCtx>;

