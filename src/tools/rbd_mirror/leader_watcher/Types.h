// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_LEADER_WATCHER_TYPES_H
#define RBD_MIRROR_LEADER_WATCHER_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <boost/variant.hpp>

namespace ceph { class Formatter; }

namespace rbd {
namespace mirror {
namespace leader_watcher {

enum NotifyOp {
  NOTIFY_OP_HEARTBEAT        = 0,
  NOTIFY_OP_LOCK_ACQUIRED    = 1,
  NOTIFY_OP_LOCK_RELEASED    = 2,
  NOTIFY_OP_SYNC_REQUEST     = 3,
  NOTIFY_OP_SYNC_REQUEST_ACK = 4,
  NOTIFY_OP_SYNC_START       = 5,
  NOTIFY_OP_SYNC_COMPLETE    = 6,
};

struct HeartbeatPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_HEARTBEAT;

  HeartbeatPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct LockAcquiredPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_LOCK_ACQUIRED;

  LockAcquiredPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct LockReleasedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_LOCK_RELEASED;

  LockReleasedPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct SyncPayloadBase {
  std::string instance_id;
  std::string request_id;

  SyncPayloadBase() {
  }

  SyncPayloadBase(const std::string &instance_id, const std::string &request_id)
    : instance_id(instance_id), request_id(request_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct SyncRequestPayload : public SyncPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SYNC_REQUEST;

  SyncRequestPayload() : SyncPayloadBase() {
  }

  SyncRequestPayload(const std::string &instance_id,
                     const std::string &request_id)
    : SyncPayloadBase(instance_id, request_id) {
  }
};

struct SyncRequestAckPayload : public SyncPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SYNC_REQUEST_ACK;

  SyncRequestAckPayload() : SyncPayloadBase() {
  }

  SyncRequestAckPayload(const std::string &instance_id,
                        const std::string &request_id)
    : SyncPayloadBase(instance_id, request_id) {
  }
};

struct SyncStartPayload : public SyncPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SYNC_START;

  SyncStartPayload() : SyncPayloadBase() {
  }

  SyncStartPayload(const std::string &instance_id,
                   const std::string &request_id)
    : SyncPayloadBase(instance_id, request_id) {
  }
};

struct SyncCompletePayload : public SyncPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SYNC_COMPLETE;

  SyncCompletePayload() : SyncPayloadBase() {
  }

  SyncCompletePayload(const std::string &instance_id,
                      const std::string &request_id)
    : SyncPayloadBase(instance_id, request_id) {
  }
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);

  UnknownPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<HeartbeatPayload,
                       LockAcquiredPayload,
                       LockReleasedPayload,
                       SyncRequestPayload,
                       SyncRequestAckPayload,
                       SyncStartPayload,
                       SyncCompletePayload,
                       UnknownPayload> Payload;

struct NotifyMessage {
  NotifyMessage(const Payload &payload = UnknownPayload()) : payload(payload) {
  }

  Payload payload;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<NotifyMessage *> &o);
};

WRITE_CLASS_ENCODER(NotifyMessage);

std::ostream &operator<<(std::ostream &out, const NotifyOp &op);

} // namespace leader_watcher
} // namespace mirror
} // namespace librbd

using rbd::mirror::leader_watcher::encode;
using rbd::mirror::leader_watcher::decode;

#endif // RBD_MIRROR_LEADER_WATCHER_TYPES_H
