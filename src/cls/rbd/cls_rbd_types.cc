// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/variant.hpp>
#include "cls/rbd/cls_rbd_types.h"
#include "common/Formatter.h"

namespace cls {
namespace rbd {

void MirrorPeer::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(uuid, bl);
  ::encode(cluster_name, bl);
  ::encode(client_name, bl);
  ::encode(pool_id, bl);
  ENCODE_FINISH(bl);
}

void MirrorPeer::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(uuid, it);
  ::decode(cluster_name, it);
  ::decode(client_name, it);
  ::decode(pool_id, it);
  DECODE_FINISH(it);
}

void MirrorPeer::dump(Formatter *f) const {
  f->dump_string("uuid", uuid);
  f->dump_string("cluster_name", cluster_name);
  f->dump_string("client_name", client_name);
  f->dump_int("pool_id", pool_id);
}

void MirrorPeer::generate_test_instances(std::list<MirrorPeer*> &o) {
  o.push_back(new MirrorPeer());
  o.push_back(new MirrorPeer("uuid-123", "cluster name", "client name", 123));
}

bool MirrorPeer::operator==(const MirrorPeer &rhs) const {
  return (uuid == rhs.uuid &&
          cluster_name == rhs.cluster_name &&
          client_name == rhs.client_name &&
          pool_id == rhs.pool_id);
}

std::ostream& operator<<(std::ostream& os, const MirrorMode& mirror_mode) {
  switch (mirror_mode) {
  case MIRROR_MODE_DISABLED:
    os << "disabled";
    break;
  case MIRROR_MODE_IMAGE:
    os << "image";
    break;
  case MIRROR_MODE_POOL:
    os << "pool";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(mirror_mode) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer) {
  os << "["
     << "uuid=" << peer.uuid << ", "
     << "cluster_name=" << peer.cluster_name << ", "
     << "client_name=" << peer.client_name;
  if (peer.pool_id != -1) {
    os << ", pool_id=" << peer.pool_id;
  }
  os << "]";
  return os;
}

void MirrorImage::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(global_image_id, bl);
  ::encode(static_cast<uint8_t>(state), bl);
  ENCODE_FINISH(bl);
}

void MirrorImage::decode(bufferlist::iterator &it) {
  uint8_t int_state;
  DECODE_START(1, it);
  ::decode(global_image_id, it);
  ::decode(int_state, it);
  state = static_cast<MirrorImageState>(int_state);
  DECODE_FINISH(it);
}

void MirrorImage::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
  f->dump_int("state", state);
}

void MirrorImage::generate_test_instances(std::list<MirrorImage*> &o) {
  o.push_back(new MirrorImage());
  o.push_back(new MirrorImage("uuid-123", MIRROR_IMAGE_STATE_ENABLED));
  o.push_back(new MirrorImage("uuid-abc", MIRROR_IMAGE_STATE_DISABLING));
}

bool MirrorImage::operator==(const MirrorImage &rhs) const {
  return global_image_id == rhs.global_image_id && state == rhs.state;
}

bool MirrorImage::operator<(const MirrorImage &rhs) const {
  return global_image_id < rhs.global_image_id ||
	(global_image_id == rhs.global_image_id  && state < rhs.state);
}

std::ostream& operator<<(std::ostream& os, const MirrorImageState& mirror_state) {
  switch (mirror_state) {
  case MIRROR_IMAGE_STATE_DISABLING:
    os << "disabling";
    break;
  case MIRROR_IMAGE_STATE_ENABLED:
    os << "enabled";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(mirror_state) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorImage& mirror_image) {
  os << "["
     << "global_image_id=" << mirror_image.global_image_id << ", "
     << "state=" << mirror_image.state << "]";
  return os;
}

void MirrorImageStatus::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(state, bl);
  ::encode(description, bl);
  ::encode(last_update, bl);
  ::encode(up, bl);
  ENCODE_FINISH(bl);
}

void MirrorImageStatus::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(state, it);
  ::decode(description, it);
  ::decode(last_update, it);
  ::decode(up, it);
  DECODE_FINISH(it);
}

void MirrorImageStatus::dump(Formatter *f) const {
  f->dump_string("state", state_to_string());
  f->dump_string("description", description);
  f->dump_stream("last_update") << last_update;
}

std::string MirrorImageStatus::state_to_string() const {
  std::stringstream ss;
  ss << (up ? "up+" : "down+") << state;
  return ss.str();
}

void MirrorImageStatus::generate_test_instances(
  std::list<MirrorImageStatus*> &o) {
  o.push_back(new MirrorImageStatus());
  o.push_back(new MirrorImageStatus(MIRROR_IMAGE_STATUS_STATE_REPLAYING));
  o.push_back(new MirrorImageStatus(MIRROR_IMAGE_STATUS_STATE_ERROR, "error"));
}

bool MirrorImageStatus::operator==(const MirrorImageStatus &rhs) const {
  return state == rhs.state && description == rhs.description && up == rhs.up;
}

std::ostream& operator<<(std::ostream& os, const MirrorImageStatusState& state) {
  switch (state) {
  case MIRROR_IMAGE_STATUS_STATE_UNKNOWN:
    os << "unknown";
    break;
  case MIRROR_IMAGE_STATUS_STATE_ERROR:
    os << "error";
    break;
  case MIRROR_IMAGE_STATUS_STATE_SYNCING:
    os << "syncing";
    break;
  case MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY:
    os << "starting_replay";
    break;
  case MIRROR_IMAGE_STATUS_STATE_REPLAYING:
    os << "replaying";
    break;
  case MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY:
    os << "stopping_replay";
    break;
  case MIRROR_IMAGE_STATUS_STATE_STOPPED:
    os << "stopped";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorImageStatus& status) {
  os << "["
     << "state=" << status.state_to_string() << ", "
     << "description=" << status.description << ", "
     << "last_update=" << status.last_update << "]";
  return os;
}

void GroupImageSpec::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(image_id, bl);
  ::encode(pool_id, bl);
  ENCODE_FINISH(bl);
}

void GroupImageSpec::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(image_id, it);
  ::decode(pool_id, it);
  DECODE_FINISH(it);
}

void GroupImageSpec::dump(Formatter *f) const {
  f->dump_string("image_id", image_id);
  f->dump_int("pool_id", pool_id);
}

int GroupImageSpec::from_key(const std::string &image_key,
				    GroupImageSpec *spec) {
  if (nullptr == spec) return -EINVAL;
  int prefix_len = cls::rbd::RBD_GROUP_IMAGE_KEY_PREFIX.size();
  std::string data_string = image_key.substr(prefix_len,
					     image_key.size() - prefix_len);
  size_t p = data_string.find("_");
  if (std::string::npos == p) {
    return -EIO;
  }
  data_string[p] = ' ';

  istringstream iss(data_string);
  uint64_t pool_id;
  string image_id;
  iss >> std::hex >> pool_id >> image_id;

  spec->image_id = image_id;
  spec->pool_id = pool_id;
  return 0;
}

std::string GroupImageSpec::image_key() {
  if (-1 == pool_id)
    return "";
  else {
    ostringstream oss;
    oss << RBD_GROUP_IMAGE_KEY_PREFIX << std::setw(16)
	<< std::setfill('0') << std::hex << pool_id << "_" << image_id;
    return oss.str();
  }
}

void GroupImageStatus::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(spec, bl);
  ::encode(state, bl);
  ENCODE_FINISH(bl);
}

void GroupImageStatus::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(spec, it);
  ::decode(state, it);
  DECODE_FINISH(it);
}

std::string GroupImageStatus::state_to_string() const {
  std::stringstream ss;
  if (state == GROUP_IMAGE_LINK_STATE_INCOMPLETE) {
    ss << "incomplete";
  }
  if (state == GROUP_IMAGE_LINK_STATE_ATTACHED) {
    ss << "attached";
  }
  return ss.str();
}

void GroupImageStatus::dump(Formatter *f) const {
  spec.dump(f);
  f->dump_string("state", state_to_string());
}

void GroupSpec::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(pool_id, bl);
  ::encode(group_id, bl);
  ENCODE_FINISH(bl);
}

void GroupSpec::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(pool_id, it);
  ::decode(group_id, it);
  DECODE_FINISH(it);
}

void GroupSpec::dump(Formatter *f) const {
  f->dump_string("group_id", group_id);
  f->dump_int("pool_id", pool_id);
}

bool GroupSpec::is_valid() const {
  return (!group_id.empty()) && (pool_id != -1);
}

void GroupSnapshotNamespace::encode(bufferlist& bl) const {
  ::encode(group_pool, bl);
  ::encode(group_id, bl);
  ::encode(snapshot_id, bl);
}

void GroupSnapshotNamespace::decode(bufferlist::iterator& it) {
  ::decode(group_pool, it);
  ::decode(group_id, it);
  ::decode(snapshot_id, it);
}

void GroupSnapshotNamespace::dump(Formatter *f) const {
  f->dump_int("group_pool", group_pool);
  f->dump_string("group_id", group_id);
  f->dump_int("snapshot_id", snapshot_id);
}

class EncodeSnapshotNamespaceVisitor : public boost::static_visitor<void> {
public:
  explicit EncodeSnapshotNamespaceVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename T>
  inline void operator()(const T& t) const {
    ::encode(static_cast<uint32_t>(T::SNAPSHOT_NAMESPACE_TYPE), m_bl);
    t.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodeSnapshotNamespaceVisitor : public boost::static_visitor<void> {
public:
  DecodeSnapshotNamespaceVisitor(bufferlist::iterator &iter)
    : m_iter(iter) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    t.decode(m_iter);
  }
private:
  bufferlist::iterator &m_iter;
};

class DumpSnapshotNamespaceVisitor : public boost::static_visitor<void> {
public:
  explicit DumpSnapshotNamespaceVisitor(Formatter *formatter, const std::string &key)
    : m_formatter(formatter), m_key(key) {}

  template <typename T>
  inline void operator()(const T& t) const {
    auto type = T::SNAPSHOT_NAMESPACE_TYPE;
    m_formatter->dump_string(m_key.c_str(), stringify(type));
    t.dump(m_formatter);
  }
private:
  ceph::Formatter *m_formatter;
  std::string m_key;
};

class GetTypeVisitor : public boost::static_visitor<SnapshotNamespaceType> {
public:
  template <typename T>
  inline SnapshotNamespaceType operator()(const T&) const {
    return static_cast<SnapshotNamespaceType>(T::SNAPSHOT_NAMESPACE_TYPE);
  }
};


SnapshotNamespaceType SnapshotNamespaceOnDisk::get_namespace_type() const {
  return static_cast<SnapshotNamespaceType>(boost::apply_visitor(GetTypeVisitor(),
								 snapshot_namespace));
}

void SnapshotNamespaceOnDisk::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodeSnapshotNamespaceVisitor(bl), snapshot_namespace);
  ENCODE_FINISH(bl);
}

void SnapshotNamespaceOnDisk::decode(bufferlist::iterator &p)
{
  DECODE_START(1, p);
  uint32_t snap_type;
  ::decode(snap_type, p);
  switch (snap_type) {
    case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER:
      snapshot_namespace = UserSnapshotNamespace();
      break;
    case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_GROUP:
      snapshot_namespace = GroupSnapshotNamespace();
      break;
    default:
      snapshot_namespace = UnknownSnapshotNamespace();
      break;
  }
  boost::apply_visitor(DecodeSnapshotNamespaceVisitor(p), snapshot_namespace);
  DECODE_FINISH(p);
}

void SnapshotNamespaceOnDisk::dump(Formatter *f) const {
  boost::apply_visitor(DumpSnapshotNamespaceVisitor(f, "snapshot_namespace_type"), snapshot_namespace);
}

void SnapshotNamespaceOnDisk::generate_test_instances(std::list<SnapshotNamespaceOnDisk *> &o) {
  o.push_back(new SnapshotNamespaceOnDisk(UserSnapshotNamespace()));
  o.push_back(new SnapshotNamespaceOnDisk(GroupSnapshotNamespace(0, "10152ae8944a", 1)));
  o.push_back(new SnapshotNamespaceOnDisk(GroupSnapshotNamespace(5, "1018643c9869", 3)));
}

std::ostream& operator<<(std::ostream& os, const UserSnapshotNamespace& ns) {
  os << "[user]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const GroupSnapshotNamespace& ns) {
  os << "[group"
     << " group_pool=" << ns.group_pool
     << " group_id=" << ns.group_id
     << " snapshot_id=" << ns.snapshot_id << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const UnknownSnapshotNamespace& ns) {
  os << "[unknown]";
  return os;
}

void TrashImageSpec::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(source, bl);
  ::encode(name, bl);
  ::encode(deletion_time, bl);
  ::encode(deferment_end_time, bl);
  ENCODE_FINISH(bl);
}

void TrashImageSpec::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(source, it);
  ::decode(name, it);
  ::decode(deletion_time, it);
  ::decode(deferment_end_time, it);
  DECODE_FINISH(it);
}

void TrashImageSpec::dump(Formatter *f) const {
  f->dump_stream("source") << source;
  f->dump_string("name", name);
  f->dump_unsigned("deletion_time", deletion_time);
  f->dump_unsigned("deferment_end_time", deferment_end_time);
}

void MirrorImageMap::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(instance_id, bl);
  ::encode(mapped_time, bl);
  ::encode(data, bl);
  ENCODE_FINISH(bl);
}

void MirrorImageMap::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(instance_id, it);
  ::decode(mapped_time, it);
  ::decode(data, it);
  DECODE_FINISH(it);
}

void MirrorImageMap::dump(Formatter *f) const {
  f->dump_string("instance_id", instance_id);
  f->dump_stream("mapped_time") << mapped_time;

  std::stringstream data_ss;
  data.hexdump(data_ss);
  f->dump_string("data", data_ss.str());
}

void MirrorImageMap::generate_test_instances(
  std::list<MirrorImageMap*> &o) {
  bufferlist data;
  data.append(std::string(128, '1'));

  o.push_back(new MirrorImageMap("uuid-123", utime_t(), data));
  o.push_back(new MirrorImageMap("uuid-abc", utime_t(), data));
}

bool MirrorImageMap::operator==(const MirrorImageMap &rhs) const {
  return instance_id == rhs.instance_id && mapped_time == rhs.mapped_time &&
    data.contents_equal(rhs.data);
}

bool MirrorImageMap::operator<(const MirrorImageMap &rhs) const {
  return instance_id < rhs.instance_id ||
        (instance_id == rhs.instance_id && mapped_time < rhs.mapped_time);
}

std::ostream& operator<<(std::ostream& os,
                         const MirrorImageMap &image_map) {
  return os << "[" << "instance_id=" << image_map.instance_id << ", mapped_time="
            << image_map.mapped_time << "]";
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationType& migration_type) {
  switch (migration_type) {
  case MIGRATION_TYPE_SRC:
    os << "source";
    break;
  case MIGRATION_TYPE_DST:
    os << "destination";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(migration_type) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationState& migration_state) {
  switch (migration_state) {
  case MIGRATION_STATE_STARTED:
    os << "started";
    break;
  case MIGRATION_STATE_COMPLETE:
    os << "complete";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(migration_state) << ")";
    break;
  }
  return os;
}

void MigrationSpec::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(type, bl);
  ::encode(state, bl);
  ::encode(pool_id, bl);
  ::encode(image_name, bl);
  ::encode(image_id, bl);
  ::encode(snap_seqs, bl);
  ::encode(overlap, bl);
  ::encode(mirroring, bl);
  ENCODE_FINISH(bl);
}

void MigrationSpec::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(type, bl);
  ::decode(state, bl);
  ::decode(pool_id, bl);
  ::decode(image_name, bl);
  ::decode(image_id, bl);
  ::decode(snap_seqs, bl);
  ::decode(overlap, bl);
  ::decode(mirroring, bl);
  DECODE_FINISH(bl);
}

std::ostream& operator<<(std::ostream& os,
                         const std::map<uint64_t, uint64_t>& snap_seqs) {
  os << "{";
  size_t count = 0;
  for (auto &it : snap_seqs) {
    os << "(" << (count++ > 0 ? ", " : "") << it.first << ", " << it.second
       << ")";
  }
  os << "}";
  return os;
}

void MigrationSpec::dump(Formatter *f) const {
  f->dump_stream("type") << type;
  f->dump_stream("state") << state;
  f->dump_int("pool_id", pool_id);
  f->dump_string("image_name", image_name);
  f->dump_string("image_id", image_id);
  f->dump_stream("snap_seqs") << snap_seqs;
  f->dump_unsigned("overlap", overlap);
  f->dump_bool("mirroring", mirroring);
}

void MigrationSpec::generate_test_instances(std::list<MigrationSpec*> &o) {
  o.push_back(new MigrationSpec());
  o.push_back(new MigrationSpec(MIGRATION_TYPE_SRC, MIGRATION_STATE_STARTED, 1,
                                "image_name", "image_id", {{1, 2}}, 123, true));
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationSpec& migration_spec) {
  os << "["
     << "type=" << migration_spec.type << ", "
     << "state=" << migration_spec.state << ", "
     << "pool_id=" << migration_spec.pool_id << ", "
     << "image_name=" << migration_spec.image_name << ", "
     << "image_id=" << migration_spec.image_id << ", "
     << "snap_seqs=" << migration_spec.snap_seqs << ", "
     << "overlap=" << migration_spec.overlap << ", "
     << "mirroring=" << migration_spec.mirroring << "]";
  return os;
}

} // namespace rbd
} // namespace cls
