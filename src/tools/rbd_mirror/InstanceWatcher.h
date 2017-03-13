// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_INSTANCE_WATCHER_H
#define CEPH_RBD_MIRROR_INSTANCE_WATCHER_H

#include <string>
#include <vector>
#include <boost/optional.hpp>

#include "librbd/Watcher.h"
#include "librbd/managed_lock/Types.h"
#include "tools/rbd_mirror/instance_watcher/Types.h"

namespace librbd {
  class ImageCtx;
  template <typename> class ManagedLock;
}

namespace rbd {
namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceWatcher : protected librbd::Watcher {
public:
  struct Listener {
    virtual ~Listener() {
    }

    virtual void image_acquire_handler(
      const std::string &global_image_id,
      const instance_watcher::ImagePeers &peers) = 0;
    virtual void image_acquired_handler(const std::string &global_image_id) = 0;
    virtual void image_release_handler(const std::string &global_image_id,
                                       bool schedule_delete) = 0;
    virtual void image_released_handler(const std::string &global_image_id) = 0;
  };

  static void get_instances(librados::IoCtx &io_ctx,
                            std::vector<std::string> *instance_ids,
                            Context *on_finish);
  static void remove_instance(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                              const std::string &instance_id,
                              Context *on_finish);

  static InstanceWatcher *create(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                                 Listener *listener) {
    return new InstanceWatcher(io_ctx, work_queue, listener, boost::none);
  }
  void destroy() {
    delete this;
  }

  InstanceWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                  Listener *listener, const boost::optional<std::string> &id);
  ~InstanceWatcher() override;

  int init();
  void shut_down();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);
  void remove(Context *on_finish);

  void notify_image_acquire(const std::string &instance_id,
                            const std::string &global_image_id,
                            const instance_watcher::ImagePeers& peers);
  void notify_image_acquired(const std::string &instance_id,
                             const std::string &global_image_id);
  void notify_image_release(const std::string &instance_id,
                            const std::string &global_image_id,
                            bool schedule_delete);
  void notify_image_released(const std::string &instance_id,
                             const std::string &global_image_id);

private:
  /**
   * @verbatim
   *
   *       BREAK_INSTANCE_LOCK -------\
   *          ^                       |
   *          |               (error) |
   *       GET_INSTANCE_LOCKER  * * *>|
   *          ^ (remove)              |
   *          |                       |
   * <uninitialized> <----------------+--------\
   *    | (init)         ^            |        |
   *    v        (error) *            |        |
   * REGISTER_INSTANCE * *     * * * *|* *> UNREGISTER_INSTANCE
   *    |                      *      |        ^
   *    v              (error) *      v        |
   * CREATE_INSTANCE_OBJECT  * *   * * * *> REMOVE_INSTANCE_OBJECT
   *    |                          *           ^
   *    v           (error)        *           |
   * REGISTER_WATCH  * * * * * * * *   * *> UNREGISTER_WATCH
   *    |                              *       ^
   *    v         (error)              *       |
   * ACQUIRE_LOCK  * * * * * * * * * * *    RELEASE_LOCK
   *    |                                      ^
   *    v       (shut_down)                    |
   * <watching> -------------------------------/
   *
   * @endverbatim
   */

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    InstanceWatcher *instance_watcher;
    Context *on_notify_ack;

    HandlePayloadVisitor(InstanceWatcher *instance_watcher,
                         Context *on_notify_ack)
      : instance_watcher(instance_watcher), on_notify_ack(on_notify_ack) {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      instance_watcher->handle_payload(payload, on_notify_ack);
    }
  };

  Listener *m_listener;
  std::string m_instance_id;

  mutable Mutex m_lock;
  librbd::ManagedLock<ImageCtxT> *m_instance_lock;
  Context *m_on_finish = nullptr;
  int m_ret_val = 0;
  bool m_removing = false;
  librbd::managed_lock::Locker m_instance_locker;

  void register_instance();
  void handle_register_instance(int r);

  void create_instance_object();
  void handle_create_instance_object(int r);

  void register_watch();
  void handle_register_watch(int r);

  void acquire_lock();
  void handle_acquire_lock(int r);

  void release_lock();
  void handle_release_lock(int r);

  void unregister_watch();
  void handle_unregister_watch(int r);

  void remove_instance_object();
  void handle_remove_instance_object(int r);

  void unregister_instance();
  void handle_unregister_instance(int r);

  void get_instance_locker();
  void handle_get_instance_locker(int r);

  void break_instance_lock();
  void handle_break_instance_lock(int r);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl) override;

  void handle_image_acquire(const std::string &global_image_id,
                            const instance_watcher::ImagePeers& peers);
  void handle_image_acquired(const std::string &global_image_id);
  void handle_image_release(const std::string &global_image_id,
                            bool schedule_delete);
  void handle_image_released(const std::string &global_image_id);

  void handle_payload(const instance_watcher::ImageAcquirePayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const instance_watcher::ImageAcquiredPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const instance_watcher::ImageReleasePayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const instance_watcher::ImageReleasedPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const instance_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);

};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_INSTANCE_WATCHER_H
