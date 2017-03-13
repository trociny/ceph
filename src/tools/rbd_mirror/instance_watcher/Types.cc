// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace rbd {
namespace mirror {
namespace instance_watcher {

namespace {

class EncodePayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    ::encode(static_cast<uint32_t>(Payload::NOTIFY_OP), m_bl);
    payload.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodePayloadVisitor : public boost::static_visitor<void> {
public:
  DecodePayloadVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {}

  template <typename Payload>
  inline void operator()(Payload &payload) const {
    payload.decode(m_version, m_iter);
  }

private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};

class DumpPayloadVisitor : public boost::static_visitor<void> {
public:
  explicit DumpPayloadVisitor(Formatter *formatter) : m_formatter(formatter) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    NotifyOp notify_op = Payload::NOTIFY_OP;
    m_formatter->dump_string("notify_op", stringify(notify_op));
    payload.dump(m_formatter);
  }

private:
  ceph::Formatter *m_formatter;
};

} // anonymous namespace

void ImagePeer::encode(bufferlist &bl) const {
  ::encode(mirror_uuid, bl);
  ::encode(image_id, bl);
  ::encode(pool_id, bl);
}

void ImagePeer::decode(bufferlist::iterator &iter) {
  ::decode(mirror_uuid, iter);
  ::decode(image_id, iter);
  ::decode(pool_id, iter);
}

void ImagePeer::dump(Formatter *f) const {
  f->dump_string("mirror_uuid", mirror_uuid);
  f->dump_string("image_id", image_id);
  f->dump_int("pool_id", pool_id);
}

void ImageAcquirePayload::encode(bufferlist &bl) const {
  ::encode(global_image_id, bl);
  ::encode(peers, bl);
}

void ImageAcquirePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(global_image_id, iter);
  ::decode(peers, iter);
}

void ImageAcquirePayload::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
  f->open_array_section("peers");
  for (auto &peer : peers) {
    f->open_object_section("peer");
    peer.dump(f);
    f->close_section();
  }
  f->close_section();
}

void ImageAcquiredPayload::encode(bufferlist &bl) const {
  ::encode(global_image_id, bl);
}

void ImageAcquiredPayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(global_image_id, iter);
}

void ImageAcquiredPayload::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
}

void ImageReleasePayload::encode(bufferlist &bl) const {
  ::encode(global_image_id, bl);
  ::encode(schedule_delete, bl);
}

void ImageReleasePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(global_image_id, iter);
  ::decode(schedule_delete, iter);
}

void ImageReleasePayload::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
  f->dump_bool("schedule_delete", schedule_delete);
}

void ImageReleasedPayload::encode(bufferlist &bl) const {
  ::encode(global_image_id, bl);
}

void ImageReleasedPayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(global_image_id, iter);
}

void ImageReleasedPayload::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
}

void UnknownPayload::encode(bufferlist &bl) const {
  assert(false);
}

void UnknownPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
}

void NotifyMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodePayloadVisitor(bl), payload);
  ENCODE_FINISH(bl);
}

void NotifyMessage::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);

  uint32_t notify_op;
  ::decode(notify_op, iter);

  // select the correct payload variant based upon the encoded op
  switch (notify_op) {
  case NOTIFY_OP_IMAGE_ACQUIRE:
    payload = ImageAcquirePayload();
    break;
  case NOTIFY_OP_IMAGE_ACQUIRED:
    payload = ImageAcquiredPayload();
    break;
  case NOTIFY_OP_IMAGE_RELEASE:
    payload = ImageReleasePayload();
    break;
  case NOTIFY_OP_IMAGE_RELEASED:
    payload = ImageReleasedPayload();
    break;
  default:
    payload = UnknownPayload();
    break;
  }

  apply_visitor(DecodePayloadVisitor(struct_v, iter), payload);
  DECODE_FINISH(iter);
}

void NotifyMessage::dump(Formatter *f) const {
  apply_visitor(DumpPayloadVisitor(f), payload);
}

void NotifyMessage::generate_test_instances(std::list<NotifyMessage *> &o) {
  o.push_back(new NotifyMessage(ImageAcquirePayload()));
  o.push_back(new NotifyMessage(ImageAcquirePayload("global_image_id",
                                                    ImagePeers())));

  o.push_back(new NotifyMessage(ImageAcquiredPayload()));
  o.push_back(new NotifyMessage(ImageAcquiredPayload("global_image_id")));

  o.push_back(new NotifyMessage(ImageReleasePayload()));
  o.push_back(new NotifyMessage(ImageReleasePayload("global_image_id", true)));

  o.push_back(new NotifyMessage(ImageReleasedPayload()));
  o.push_back(new NotifyMessage(ImageReleasedPayload("global_image_id")));
}

std::ostream &operator<<(std::ostream &out, const NotifyOp &op) {
  switch (op) {
  case NOTIFY_OP_IMAGE_ACQUIRE:
    out << "ImageAcquire";
    break;
  case NOTIFY_OP_IMAGE_ACQUIRED:
    out << "ImageAcquired";
    break;
  case NOTIFY_OP_IMAGE_RELEASE:
    out << "ImageRelease";
    break;
  case NOTIFY_OP_IMAGE_RELEASED:
    out << "ImageReleased";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

} // namespace instance_watcher
} // namespace mirror
} // namespace librbd
