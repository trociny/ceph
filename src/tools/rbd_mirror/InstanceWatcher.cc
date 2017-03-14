// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "InstanceWatcher.h"
#include "include/stringify.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ManagedLock.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::InstanceWatcher: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

using namespace instance_watcher;

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

namespace {

struct C_GetInstances : public Context {
  std::vector<std::string> *instance_ids;
  Context *on_finish;
  bufferlist out_bl;

  C_GetInstances(std::vector<std::string> *instance_ids, Context *on_finish)
    : instance_ids(instance_ids), on_finish(on_finish) {
  }

  void finish(int r) override {
    if (r == 0) {
      bufferlist::iterator it = out_bl.begin();
      r = librbd::cls_client::mirror_instances_list_finish(&it, instance_ids);
    } else if (r == -ENOENT) {
      r = 0;
    }
    on_finish->complete(r);
  }
};

template <typename I>
struct RemoveInstanceRequest : public Context {
  InstanceWatcher<I> instance_watcher;
  Context *on_finish;

  RemoveInstanceRequest(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                        const std::string &instance_id, Context *on_finish)
    : instance_watcher(io_ctx, work_queue, nullptr, instance_id),
      on_finish(on_finish) {
  }

  void send() {
    instance_watcher.remove(this);
  }

  void finish(int r) override {
    assert(r == 0);

    on_finish->complete(r);
  }
};

struct NotifyInstanceRequest : public Context {
  librbd::watcher::Notifier notifier;
  bufferlist bl;

  NotifyInstanceRequest(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                        const std::string &instance_id, bufferlist &&bl)
    : notifier(work_queue, io_ctx, RBD_MIRROR_INSTANCE_PREFIX + instance_id),
      bl(bl) {
  }

  void send() {
    notifier.notify(bl, nullptr, this);
  }

  void finish(int r) override {
  }
};

} // anonymous namespace

template <typename I>
void InstanceWatcher<I>::get_instances(librados::IoCtx &io_ctx,
                                       std::vector<std::string> *instance_ids,
                                       Context *on_finish) {
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_instances_list_start(&op);
  C_GetInstances *ctx = new C_GetInstances(instance_ids, on_finish);
  librados::AioCompletion *aio_comp = create_rados_callback(ctx);

  int r = io_ctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op, &ctx->out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::remove_instance(librados::IoCtx &io_ctx,
                                         ContextWQ *work_queue,
                                         const std::string &instance_id,
                                         Context *on_finish) {
  auto req = new RemoveInstanceRequest<I>(io_ctx, work_queue, instance_id,
                                          on_finish);
  req->send();
}

template <typename I>
InstanceWatcher<I>::InstanceWatcher(librados::IoCtx &io_ctx,
                                    ContextWQ *work_queue, Listener *listener,
                                    const boost::optional<std::string> &id)
  : Watcher(io_ctx, work_queue, RBD_MIRROR_INSTANCE_PREFIX +
            (id ? *id : stringify(io_ctx.get_instance_id()))),
    m_listener(listener),
    m_instance_id(id ? *id : stringify(io_ctx.get_instance_id())),
    m_lock("rbd::mirror::InstanceWatcher " + io_ctx.get_pool_name()),
    m_instance_lock(librbd::ManagedLock<I>::create(
      m_ioctx, m_work_queue, m_oid, this, librbd::managed_lock::EXCLUSIVE, true,
      m_cct->_conf->rbd_blacklist_expire_seconds)) {
}

template <typename I>
InstanceWatcher<I>::~InstanceWatcher() {
  m_instance_lock->destroy();
}

template <typename I>
int InstanceWatcher<I>::init() {
  C_SaferCond init_ctx;
  init(&init_ctx);
  return init_ctx.wait();
}

template <typename I>
void InstanceWatcher<I>::init(Context *on_finish) {
  dout(20) << "instance_id=" << m_instance_id << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  register_instance();
}

template <typename I>
void InstanceWatcher<I>::shut_down() {
  C_SaferCond shut_down_ctx;
  shut_down(&shut_down_ctx);
  int r = shut_down_ctx.wait();
  assert(r == 0);
}

template <typename I>
void InstanceWatcher<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  release_lock();
}

template <typename I>
void InstanceWatcher<I>::remove(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;
  m_removing = true;

  get_instance_locker();
}

template <typename I>
void InstanceWatcher<I>::notify_image_acquire(
  const std::string &instance_id, const std::string &global_image_id,
  const ImagePeers& peers) {
  dout(20) << "instance_id=" << instance_id << dendl;

  Mutex::Locker locker(m_lock);

  if (m_on_finish != nullptr) {
    return;
  }

  // XXXMG: AsyncOpTracker

  if (instance_id == m_instance_id) {
    handle_image_acquire(global_image_id, peers);
  } else {
    bufferlist bl;
    ::encode(NotifyMessage{ImageAcquirePayload{global_image_id, peers}}, bl);
    auto req = new NotifyInstanceRequest(
      m_ioctx, m_work_queue, instance_id, std::move(bl));
    req->send();
  }
}

template <typename I>
void InstanceWatcher<I>::notify_image_acquired(
  const std::string &instance_id, const std::string &global_image_id) {
  dout(20) << "instance_id=" << instance_id << dendl;

  Mutex::Locker locker(m_lock);

  if (m_on_finish != nullptr) {
    return;
  }

  // XXXMG: AsyncOpTracker

  if (instance_id == m_instance_id) {
    handle_image_acquired(global_image_id);
  } else {
    bufferlist bl;
    ::encode(NotifyMessage{ImageAcquiredPayload{global_image_id}}, bl);
    auto req = new NotifyInstanceRequest(
      m_ioctx, m_work_queue, instance_id, std::move(bl));
    req->send();
  }
}

template <typename I>
void InstanceWatcher<I>::notify_image_release(
  const std::string &instance_id, const std::string &global_image_id,
  bool schedule_delete) {
  dout(20) << "instance_id=" << instance_id << dendl;

  Mutex::Locker locker(m_lock);

  if (m_on_finish != nullptr) {
    return;
  }

  // XXXMG: AsyncOpTracker

  if (instance_id == m_instance_id) {
    handle_image_release(global_image_id, schedule_delete);
  } else {
    bufferlist bl;
    ::encode(NotifyMessage{
        ImageReleasePayload{global_image_id, schedule_delete}}, bl);
    auto req = new NotifyInstanceRequest(
      m_ioctx, m_work_queue, instance_id, std::move(bl));
    req->send();
  }
}

template <typename I>
void InstanceWatcher<I>::notify_image_released(
  const std::string &instance_id, const std::string &global_image_id) {
  dout(20) << "instance_id=" << instance_id << dendl;

  Mutex::Locker locker(m_lock);

  if (m_on_finish != nullptr) {
    return;
  }

  // XXXMG: AsyncOpTracker

  if (instance_id == m_instance_id) {
    handle_image_released(global_image_id);
  } else {
    bufferlist bl;
    ::encode(NotifyMessage{ImageReleasedPayload{global_image_id}}, bl);
    auto req = new NotifyInstanceRequest(
      m_ioctx, m_work_queue, instance_id, std::move(bl));
    req->send();
  }
}

template <typename I>
void InstanceWatcher<I>::register_instance() {
  assert(m_lock.is_locked());

  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_instances_add(&op, m_instance_id);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_register_instance>(this);

  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_register_instance(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r == 0) {
      create_instance_object();
      return;
    }

    derr << "error registering instance: " << cpp_strerror(r) << dendl;

    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}


template <typename I>
void InstanceWatcher<I>::create_instance_object() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  librados::ObjectWriteOperation op;
  op.create(true);

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>,
    &InstanceWatcher<I>::handle_create_instance_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_create_instance_object(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error creating " << m_oid << " object: " << cpp_strerror(r)
         << dendl;

    m_ret_val = r;
    unregister_instance();
    return;
  }

  register_watch();
}

template <typename I>
void InstanceWatcher<I>::register_watch() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_register_watch>(this));

  librbd::Watcher::register_watch(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_register_watch(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error registering instance watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;

    m_ret_val = r;
    remove_instance_object();
    return;
  }

  acquire_lock();
}

template <typename I>
void InstanceWatcher<I>::acquire_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_acquire_lock>(this));

  m_instance_lock->acquire_lock(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_acquire_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r < 0) {

      derr << "error acquiring instance lock: " << cpp_strerror(r) << dendl;

      m_ret_val = r;
      unregister_watch();
      return;
    }

    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void InstanceWatcher<I>::release_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_release_lock>(this));

  m_instance_lock->shut_down(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_release_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error releasing instance lock: " << cpp_strerror(r) << dendl;
  }

  unregister_watch();
}

template <typename I>
void InstanceWatcher<I>::unregister_watch() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      InstanceWatcher<I>, &InstanceWatcher<I>::handle_unregister_watch>(this));

  librbd::Watcher::unregister_watch(ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_unregister_watch(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering instance watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
  }

  Mutex::Locker locker(m_lock);
  remove_instance_object();
}

template <typename I>
void InstanceWatcher<I>::remove_instance_object() {
  assert(m_lock.is_locked());

  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>,
    &InstanceWatcher<I>::handle_remove_instance_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_remove_instance_object(int r) {
  dout(20) << "r=" << r << dendl;

  if (m_removing && r == -ENOENT) {
    r = 0;
  }

  if (r < 0) {
    derr << "error removing " << m_oid << " object: " << cpp_strerror(r)
         << dendl;
  }

  Mutex::Locker locker(m_lock);
  unregister_instance();
}

template <typename I>
void InstanceWatcher<I>::unregister_instance() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_instances_remove(&op, m_instance_id);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_unregister_instance>(this);

  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void InstanceWatcher<I>::handle_unregister_instance(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering instance: " << cpp_strerror(r) << dendl;
  }

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    std::swap(on_finish, m_on_finish);
    r = m_ret_val;

    if (m_removing) {
      m_removing = false;
    }
  }
  on_finish->complete(r);
}

template <typename I>
void InstanceWatcher<I>::get_instance_locker() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_get_instance_locker>(this));

  m_instance_lock->get_locker(&m_instance_locker, ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_get_instance_locker(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error retrieving instance locker: " << cpp_strerror(r) << dendl;
    }
    remove_instance_object();
    return;
  }

  break_instance_lock();
}

template <typename I>
void InstanceWatcher<I>::break_instance_lock() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
    InstanceWatcher<I>, &InstanceWatcher<I>::handle_break_instance_lock>(this));

  m_instance_lock->break_lock(m_instance_locker, true, ctx);
}

template <typename I>
void InstanceWatcher<I>::handle_break_instance_lock(int r) {
  dout(20) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error breaking instance lock: " << cpp_strerror(r) << dendl;
    }
    remove_instance_object();
    return;
  }

  remove_instance_object();
}

template <typename I>
void InstanceWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                       uint64_t notifier_id, bufferlist &bl) {
  dout(20) << "notify_id=" << notify_id << ", handle=" << handle << ", "
           << "notifier_id=" << notifier_id << dendl;

  Context *ctx = new librbd::watcher::C_NotifyAck(this, notify_id, handle);

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
void InstanceWatcher<I>::handle_image_acquire(
  const std::string &global_image_id,
  const ImagePeers& peers) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  m_listener->image_acquire_handler(global_image_id, peers);
}

template <typename I>
void InstanceWatcher<I>::handle_image_acquired(
  const std::string &global_image_id) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  m_listener->image_acquired_handler(global_image_id);
}

template <typename I>
void InstanceWatcher<I>::handle_image_release(
  const std::string &global_image_id, bool schedule_delete) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  m_listener->image_release_handler(global_image_id, schedule_delete);
}

template <typename I>
void InstanceWatcher<I>::handle_image_released(
  const std::string &global_image_id) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  m_listener->image_released_handler(global_image_id);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const ImageAcquirePayload &payload,
					Context *on_notify_ack) {
  dout(20) << "image_acquire" << dendl;

  handle_image_acquire(payload.global_image_id, payload.peers);
  on_notify_ack->complete(0);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const ImageAcquiredPayload &payload,
					Context *on_notify_ack) {
  dout(20) << "image_acquired" << dendl;

  handle_image_acquired(payload.global_image_id);
  on_notify_ack->complete(0);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const ImageReleasePayload &payload,
					Context *on_notify_ack) {
  dout(20) << "image_release" << dendl;

  handle_image_release(payload.global_image_id, payload.schedule_delete);
  on_notify_ack->complete(0);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const ImageReleasedPayload &payload,
					Context *on_notify_ack) {
  dout(20) << "image_released" << dendl;

  handle_image_released(payload.global_image_id);
  on_notify_ack->complete(0);
}

template <typename I>
void InstanceWatcher<I>::handle_payload(const UnknownPayload &payload,
					Context *on_notify_ack) {
  dout(20) << "unknown" << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::InstanceWatcher<librbd::ImageCtx>;
