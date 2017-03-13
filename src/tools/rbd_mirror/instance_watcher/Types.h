// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_INSTANCE_WATCHER_TYPES_H
#define RBD_MIRROR_INSTANCE_WATCHER_TYPES_H

#include <string>
#include <set>
#include <boost/variant.hpp>

#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/int_types.h"

namespace ceph { class Formatter; }

namespace rbd {
namespace mirror {
namespace instance_watcher {

struct ImagePeer {
  std::string mirror_uuid;
  std::string image_id;
  int64_t pool_id;

  inline bool operator<(const ImagePeer &rhs) const {
    return mirror_uuid < rhs.mirror_uuid;
  }

  inline bool operator==(const ImagePeer &rhs) const {
    return (mirror_uuid == rhs.mirror_uuid && image_id == rhs.image_id &&
	    pool_id == rhs.pool_id);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

WRITE_CLASS_ENCODER(ImagePeer);

typedef std::set<ImagePeer> ImagePeers;

enum NotifyOp {
  NOTIFY_OP_IMAGE_ACQUIRE  = 0,
  NOTIFY_OP_IMAGE_ACQUIRED = 1,
  NOTIFY_OP_IMAGE_RELEASE  = 2,
  NOTIFY_OP_IMAGE_RELEASED = 3,
};

struct ImageAcquirePayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_ACQUIRE;

  std::string global_image_id;
  ImagePeers peers;

  ImageAcquirePayload() {
  }

  ImageAcquirePayload(const std::string &global_image_id,
                      const ImagePeers &peers)
    : global_image_id(global_image_id), peers(peers) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImageAcquiredPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_ACQUIRED;

  std::string global_image_id;

  ImageAcquiredPayload() {
  }

  ImageAcquiredPayload(const std::string &global_image_id)
    : global_image_id(global_image_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImageReleasePayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_RELEASE;

  std::string global_image_id;
  bool schedule_delete = false;

  ImageReleasePayload() {
  }

  ImageReleasePayload(const std::string &global_image_id, bool schedule_delete)
    : global_image_id(global_image_id), schedule_delete(schedule_delete) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImageReleasedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_RELEASED;

  std::string global_image_id;

  ImageReleasedPayload() {
  }

  ImageReleasedPayload(const std::string &global_image_id)
    : global_image_id(global_image_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);

  UnknownPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<ImageAcquirePayload,
		       ImageAcquiredPayload,
                       ImageReleasePayload,
                       ImageReleasedPayload,
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

} // namespace instance_watcher
} // namespace mirror
} // namespace librbd

using rbd::mirror::instance_watcher::encode;
using rbd::mirror::instance_watcher::decode;

#endif // RBD_MIRROR_INSTANCE_WATCHER_TYPES_H
