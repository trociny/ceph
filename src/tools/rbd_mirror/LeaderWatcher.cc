// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LeaderWatcher.h"
#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "librbd/watcher/Types.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::LeaderWatcher: " \
                           << this << " " << __func__ << ": "
namespace rbd {
namespace mirror {

using namespace leader_watcher;

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
LeaderWatcher<I>::LeaderWatcher(Threads *threads, librados::IoCtx &io_ctx,
                                Listener *listener)
  : Watcher(io_ctx, threads->work_queue, RBD_MIRROR_LEADER),
    m_threads(threads), m_listener(listener),
    m_lock("rbd::mirror::LeaderWatcher " + io_ctx.get_pool_name()),
    m_notifier_id(librados::Rados(io_ctx).get_instance_id()),
    m_leader_lock(new LeaderLock(m_ioctx, m_work_queue, m_oid, this, true,
                                 m_cct->_conf->rbd_blacklist_expire_seconds)) {
}

template <typename I>
LeaderWatcher<I>::~LeaderWatcher() {
  assert(m_status_watcher == nullptr);
  assert(m_instances == nullptr);
  assert(m_timer_task == nullptr);

  delete m_leader_lock;
}

template <typename I>
int LeaderWatcher<I>::init() {
  C_SaferCond init_ctx;
  init(&init_ctx);
  return init_ctx.wait();
}

template <typename I>
void LeaderWatcher<I>::init(Context *on_finish) {
  dout(20) << "notifier_id=" << m_notifier_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  create_leader_object();
}

template <typename I>
void LeaderWatcher<I>::create_leader_object() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  librados::ObjectWriteOperation op;
  op.create(false);

  librados::AioCompletion *aio_comp = create_rados_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_create_leader_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void LeaderWatcher<I>::handle_create_leader_object(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r == 0) {
      register_watch();
      return;
    }

    derr << "error creating " << m_oid << " object: " << cpp_strerror(r)
         << dendl;

    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::register_watch() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_register_watch>(this));

  librbd::Watcher::register_watch(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_register_watch(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r < 0) {
      derr << "error registering leader watcher for " << m_oid << " object: "
           << cpp_strerror(r) << dendl;
    } else {
      acquire_leader_lock(true);
    }

    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::shut_down() {
  C_SaferCond shut_down_ctx;
  shut_down(&shut_down_ctx);
  int r = shut_down_ctx.wait();
  assert(r == 0);
}

template <typename I>
void LeaderWatcher<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  assert(m_on_shut_down_finish == nullptr);
  m_on_shut_down_finish = on_finish;
  cancel_timer_task();
  shut_down_leader_lock();
}

template <typename I>
void LeaderWatcher<I>::shut_down_leader_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_shut_down_leader_lock>(this));

  m_leader_lock->shut_down(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_shut_down_leader_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error shutting down leader lock: " << cpp_strerror(r) << dendl;
  }

  unregister_watch();
}

template <typename I>
void LeaderWatcher<I>::unregister_watch() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_unregister_watch>(this));

  librbd::Watcher::unregister_watch(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_unregister_watch(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r < 0) {
      derr << "error unregistering leader watcher for " << m_oid << " object: "
           << cpp_strerror(r) << dendl;
    }

    assert(m_on_shut_down_finish != nullptr);
    std::swap(on_finish, m_on_shut_down_finish);
  }
  on_finish->complete(0);
}

template <typename I>
bool LeaderWatcher<I>::is_leader() {
  Mutex::Locker locker(m_lock);

  return is_leader(m_lock);
}

template <typename I>
bool LeaderWatcher<I>::is_leader(Mutex &lock) {
  assert(m_lock.is_locked());

  bool leader = m_leader_lock->is_leader();
  dout(20) << leader << dendl;
  return leader;
}

template <typename I>
void LeaderWatcher<I>::release_leader() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  if (!is_leader(m_lock)) {
    return;
  }

  release_leader_lock();
}

template <typename I>
void LeaderWatcher<I>::list_instances(std::vector<std::string> *instance_ids) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  instance_ids->clear();
  if (m_instances != nullptr) {
    m_instances->list(instance_ids);
  }
}


template <typename I>
void LeaderWatcher<I>::cancel_timer_task() {
  assert(m_threads->timer_lock.is_locked());
  assert(m_lock.is_locked());

  if (m_timer_task == nullptr) {
    return;
  }

  dout(20) << m_timer_task << dendl;
  bool canceled = m_threads->timer->cancel_event(m_timer_task);
  assert(canceled);
  m_timer_task = nullptr;
}

template <typename I>
void LeaderWatcher<I>::schedule_timer_task(const std::string &name,
                                           int delay_factor, bool leader,
                                           void (LeaderWatcher<I>::*cb)()) {
  assert(m_threads->timer_lock.is_locked());
  assert(m_lock.is_locked());

  if (m_on_shut_down_finish != nullptr) {
    return;
  }

  cancel_timer_task();

  m_timer_task = new FunctionContext(
    [this, cb, leader](int r) {
      assert(m_threads->timer_lock.is_locked());
      m_timer_task = nullptr;
      Mutex::Locker locker(m_lock);
      if (is_leader(m_lock) != leader) {
        return;
      }
      (this->*cb)();
    });

  int after = delay_factor *
    max(1, m_cct->_conf->rbd_mirror_leader_heartbeat_interval);

  dout(20) << "scheduling " << name << " after " << after << " sec (task "
           << m_timer_task << ")" << dendl;
  m_threads->timer->add_event_after(after, m_timer_task);
}

template <typename I>
void LeaderWatcher<I>::handle_post_acquire_leader_lock(int r,
                                                       Context *on_finish) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -EAGAIN) {
      dout(20) << "already locked" << dendl;
    } else {
      derr << "error acquiring leader lock: " << cpp_strerror(r) << dendl;
    }
    on_finish->complete(r);
    return;
  }

  Mutex::Locker locker(m_lock);
  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  init_status_watcher();
}

template <typename I>
void LeaderWatcher<I>::handle_pre_release_leader_lock(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  notify_listener();
}

template <typename I>
void LeaderWatcher<I>::handle_post_release_leader_lock(int r,
                                                       Context *on_finish) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  Mutex::Locker locker(m_lock);
  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  notify_lock_released();
}

template <typename I>
void LeaderWatcher<I>::break_leader_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  if (m_locker.cookie.empty()) {
    acquire_leader_lock(true);
    return;
  }

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_break_leader_lock>(this));

  m_leader_lock->break_lock(m_locker, true, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_break_leader_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (m_leader_lock->is_shutdown()) {
    dout(20) << "canceling due to shutdown" << dendl;
    return;
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error beaking leader lock: " << cpp_strerror(r)  << dendl;

    schedule_timer_task("get locker", 1, false, &LeaderWatcher<I>::get_locker);
    return;
  }

  acquire_leader_lock(true);
}

template <typename I>
void LeaderWatcher<I>::get_locker() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  C_GetLocker *get_locker_ctx = new C_GetLocker(this);
  Context *ctx = create_async_context_callback(m_work_queue, get_locker_ctx);

  m_leader_lock->get_locker(&get_locker_ctx->locker, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_get_locker(int r,
                                         librbd::managed_lock::Locker& locker) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker mutex_locker(m_lock);

  if (m_leader_lock->is_shutdown()) {
    dout(20) << "canceling due to shutdown" << dendl;
    return;
  }

  if (is_leader(m_lock)) {
    m_locker = {};
  } else {
    if (r == -ENOENT) {
      acquire_leader_lock(true);
    } else {
      if (r < 0) {
        derr << "error retrieving leader locker: " << cpp_strerror(r) << dendl;
      } else {
        m_locker = locker;
      }

      schedule_timer_task("acquire leader lock",
                          m_cct->_conf->rbd_mirror_leader_max_missed_heartbeats,
                          false, &LeaderWatcher<I>::acquire_leader_lock);
    }
  }
}

template <typename I>
void LeaderWatcher<I>::acquire_leader_lock() {
    return acquire_leader_lock(false);
}

template <typename I>
void LeaderWatcher<I>::acquire_leader_lock(bool reset_attempt_counter) {
  dout(20) << "reset_attempt_counter=" << reset_attempt_counter << dendl;

  assert(m_lock.is_locked());

  if (reset_attempt_counter) {
    m_acquire_attempts = 0;
  }

  dout(20) << "acquire_attempts=" << m_acquire_attempts << dendl;

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_acquire_leader_lock>(this));

  m_leader_lock->try_acquire_lock(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_acquire_leader_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (m_leader_lock->is_shutdown()) {
    dout(20) << "canceling due to shutdown" << dendl;
    return;
  }

  if (r < 0) {
    if (r == -EAGAIN) {
      dout(20) << "already locked" << dendl;
    } else {
      derr << "error acquiring lock: " << cpp_strerror(r) << dendl;
    }
    if (++m_acquire_attempts >
        m_cct->_conf->rbd_mirror_leader_max_acquire_attempts_before_break) {
      dout(0) << "breaking leader lock after failed attemts to acquire"
              << dendl;
      break_leader_lock();
    } else {
      get_locker();
    }
    return;
  }

  m_acquire_attempts = 0;

  if (m_ret_val) {
    dout(5) << "releasing due to error on notify" << dendl;
    release_leader_lock();
    return;
  }

  notify_heartbeat();
}

template <typename I>
void LeaderWatcher<I>::release_leader_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_release_leader_lock>(this));

  m_leader_lock->release_lock(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_release_leader_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error releasing lock: " << cpp_strerror(r) << dendl;
    return;
  }

  schedule_timer_task("get locker", 1, false, &LeaderWatcher<I>::get_locker);
}

template <typename I>
void LeaderWatcher<I>::init_status_watcher() {
  dout(20) << dendl;

  assert(m_lock.is_locked());
  assert(m_status_watcher == nullptr);

  m_status_watcher = MirrorStatusWatcher<I>::create(m_ioctx, m_work_queue);

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_init_status_watcher>(this);

  m_status_watcher->init(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_init_status_watcher(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r == 0) {
      init_instances();
      return;
    }

    derr << "error initializing mirror status watcher: " << cpp_strerror(r)
         << dendl;
    m_status_watcher->destroy();
    m_status_watcher = nullptr;
    assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::shut_down_status_watcher() {
  dout(20) << dendl;

  assert(m_lock.is_locked());
  assert(m_status_watcher != nullptr);

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<LeaderWatcher<I>,
      &LeaderWatcher<I>::handle_shut_down_status_watcher>(this));

  m_status_watcher->shut_down(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_shut_down_status_watcher(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    m_status_watcher->destroy();
    m_status_watcher = nullptr;

    if (r < 0) {
      derr << "error shutting mirror status watcher down: " << cpp_strerror(r)
           << dendl;
    }

    if (m_ret_val != 0) {
      r = m_ret_val;
    }

    if (!is_leader(m_lock)) {
// ignore on releasing
      r = 0;
    }

    assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::init_instances() {
  dout(20) << dendl;

  assert(m_lock.is_locked());
  assert(m_instances == nullptr);

  m_instances = Instances<I>::create(m_threads, m_ioctx);

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_init_instances>(this);

  m_instances->init(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_init_instances(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error initializing instances: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    m_instances->destroy();
    m_instances = nullptr;
    shut_down_status_watcher();
    return;
  }

  notify_listener();
}

template <typename I>
void LeaderWatcher<I>::shut_down_instances() {
  dout(20) << dendl;

  assert(m_lock.is_locked());
  assert(m_instances != nullptr);

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<LeaderWatcher<I>,
      &LeaderWatcher<I>::handle_shut_down_instances>(this));

  m_instances->shut_down(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_shut_down_instances(int r) {
  dout(20) << "r=" << r << dendl;
  assert(r == 0);

  Mutex::Locker locker(m_lock);

  m_instances->destroy();
  m_instances = nullptr;

  shut_down_status_watcher();
}

template <typename I>
void LeaderWatcher<I>::notify_listener() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_listener>(this));

  if (is_leader(m_lock)) {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        m_listener->post_acquire_handler(ctx);
      });
  } else {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        m_listener->pre_release_handler(ctx);
      });
  }
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_listener(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error notifying listener: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  if (is_leader(m_lock)) {
    notify_lock_acquired();
  } else {
    shut_down_instances();
  }
}

template <typename I>
void LeaderWatcher<I>::notify_lock_acquired() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_lock_acquired>(this);

  bufferlist bl;
  ::encode(NotifyMessage{LockAcquiredPayload{}}, bl);

  send_notify(bl, nullptr, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_lock_acquired(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    if (r < 0 && r != -ETIMEDOUT) {
      derr << "error notifying leader lock acquired: " << cpp_strerror(r)
           << dendl;
      m_ret_val = r;
    }

    assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  }
  on_finish->complete(0);
}

template <typename I>
void LeaderWatcher<I>::notify_lock_released() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_lock_released>(this);

  bufferlist bl;
  ::encode(NotifyMessage{LockReleasedPayload{}}, bl);

  send_notify(bl, nullptr, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_lock_released(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    if (r < 0 && r != -ETIMEDOUT) {
      derr << "error notifying leader lock released: " << cpp_strerror(r)
           << dendl;
    }

    assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::notify_heartbeat() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  if (!is_leader(m_lock)) {
    dout(5) << "not leader, canceling" << dendl;
    return;
  }

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_heartbeat>(this);

  bufferlist bl;
  ::encode(NotifyMessage{HeartbeatPayload{}}, bl);

  m_heartbeat_ack_bl.clear();
  send_notify(bl, &m_heartbeat_ack_bl, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_heartbeat(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (!is_leader(m_lock)) {
    return;
  }

  if (r < 0 && r != -ETIMEDOUT) {
    derr << "error notifying hearbeat: " << cpp_strerror(r)
         <<  ", releasing leader" << dendl;
    release_leader_lock();
    return;
  }

  try {
    bufferlist::iterator iter = m_heartbeat_ack_bl.begin();
    uint32_t num_acks;
    ::decode(num_acks, iter);

    dout(20) << num_acks << " acks received" << dendl;

    for (uint32_t i = 0; i < num_acks; i++) {
      uint64_t notifier_id;
      uint64_t cookie;
      bufferlist reply_bl;

      ::decode(notifier_id, iter);
      ::decode(cookie, iter);
      ::decode(reply_bl, iter);

      if (notifier_id == m_notifier_id) {
	continue;
      }

      std::string instance_id = stringify(notifier_id);
      m_instances->notify(instance_id);
    }
  } catch (const buffer::error &err) {
    derr << ": error decoding heartbeat acks: " << err.what() << dendl;
  }

  schedule_timer_task("heartbeat", 1, true,
                      &LeaderWatcher<I>::notify_heartbeat);
}

template <typename I>
void LeaderWatcher<I>::handle_heartbeat(Context *on_notify_ack) {
  dout(20) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    if (is_leader(m_lock)) {
      dout(5) << "got another leader heartbeat, ignoring" << dendl;
    } else {
      m_acquire_attempts = 0;
      cancel_timer_task();
      get_locker();
    }
  }

  on_notify_ack->complete(0);
}

template <typename I>
void LeaderWatcher<I>::handle_lock_acquired(Context *on_notify_ack) {
  dout(20) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    if (is_leader(m_lock)) {
      dout(5) << "got another leader lock_acquired, ignoring" << dendl;
    } else {
      cancel_timer_task();
      m_acquire_attempts = 0;
      get_locker();
    }
  }

  on_notify_ack->complete(0);
}

template <typename I>
void LeaderWatcher<I>::handle_lock_released(Context *on_notify_ack) {
  dout(20) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    if (is_leader(m_lock)) {
      dout(5) << "got another leader lock_released, ignoring" << dendl;
    } else {
      cancel_timer_task();
      acquire_leader_lock(true);
    }
  }

  on_notify_ack->complete(0);
}

template <typename I>
void LeaderWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                     uint64_t notifier_id, bufferlist &bl) {
  dout(20) << "notify_id=" << notify_id << ", handle=" << handle << ", "
           << "notifier_id=" << notifier_id << dendl;

  Context *ctx = new librbd::watcher::C_NotifyAck(this, notify_id, handle);

  if (notifier_id == m_notifier_id) {
    dout(20) << "our own notification, ignoring" << dendl;
    ctx->complete(0);
    return;
  }

  NotifyMessage notify_message;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(notify_message, iter);
  } catch (const buffer::error &err) {
    derr << ": error decoding image notification: " << err.what() << dendl;
    ctx->complete(0);
    return;
  }

  apply_visitor(HandlePayloadVisitor(this, ctx), notify_message.payload);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const HeartbeatPayload &payload,
                                      Context *on_notify_ack) {
  dout(20) << "heartbeat" << dendl;

  handle_heartbeat(on_notify_ack);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const LockAcquiredPayload &payload,
                                      Context *on_notify_ack) {
  dout(20) << "lock_acquired" << dendl;

  handle_lock_acquired(on_notify_ack);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const LockReleasedPayload &payload,
                                      Context *on_notify_ack) {
  dout(20) << "lock_released" << dendl;

  handle_lock_released(on_notify_ack);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const UnknownPayload &payload,
                                      Context *on_notify_ack) {
  dout(20) << "unknown" << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::LeaderWatcher<librbd::ImageCtx>;
