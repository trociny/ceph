// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OSDPerfMetric.h"

namespace {

class GetTypeVisitor : public boost::static_visitor<OSDPerfMetricQueryType> {
public:
  template <typename T>
  inline OSDPerfMetricQueryType operator()(const T&) const {
    return T::TYPE;
  }
};

class BoundEncodeVisitor : public boost::static_visitor<void> {
public:
  BoundEncodeVisitor(size_t &p) : p(p) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    denc(static_cast<uint32_t>(T::TYPE), p);
    denc(t, p);
  }
private:
  size_t &p;
};

class EncodeVisitor : public boost::static_visitor<void> {
public:
  EncodeVisitor(buffer::list::contiguous_appender& p) : p(p) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    denc(static_cast<uint32_t>(T::TYPE), p);
    denc(t, p);
  }
private:
  buffer::list::contiguous_appender& p;
};

class DecodeVisitor : public boost::static_visitor<void> {
public:
  DecodeVisitor(buffer::ptr::const_iterator &p) : p(p) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    denc(t, p);
  }
private:
  buffer::ptr::const_iterator &p;
};

class DumpVisitor : public boost::static_visitor<void> {
public:
  explicit DumpVisitor(Formatter *formatter) : formatter(formatter) {
  }

  template <typename T>
  inline void operator()(const T& t) const {
    auto type = T::TYPE;
    formatter->dump_string("query_type", stringify(type));
    t.dump(formatter);
  }
private:
  ceph::Formatter *formatter;
};

class MakeKeyVisitor : public boost::static_visitor<bool> {
public:
  explicit MakeKeyVisitor(const MOSDOp *m, std::string *key) : m(m), key(key) {
  }

  template <typename T>
  inline bool operator()(const T& t) const {
    return t(m, key);
  }
private:
  const MOSDOp *m;
  std::string *key;
};

class PrintVisitor : public boost::static_visitor<std::ostream &> {
public:
  explicit PrintVisitor(std::ostream &out) : out(out) {
  }

  template <typename T>
  inline std::ostream & operator()(const T& t) const {
    auto type = T::TYPE;
    return out << type;
  }
private:
  std::ostream &out;
};

} // anonymous namespace

std::ostream &operator<<(std::ostream &out, const OSDPerfMetricQueryType &type) {
  switch (type) {
  case OSD_PERF_QUERY_TYPE_CLIENT_ID:
    out << "GROUP BY client_id";
    break;
  case OSD_PERF_QUERY_TYPE_RBD_IMAGE_ID:
    out << "GROUP BY RBD image_id";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return out;
}

OSDPerfMetricQueryType OSDPerfMetricQueryEntry::get_query_type() const {
  return boost::apply_visitor(GetTypeVisitor(), query);
}

void OSDPerfMetricQueryEntry::bound_encode(size_t &p) const {
  boost::apply_visitor(BoundEncodeVisitor(p), query);
}

void OSDPerfMetricQueryEntry::encode(buffer::list::contiguous_appender &p) const {
  boost::apply_visitor(EncodeVisitor(p), query);
}

void OSDPerfMetricQueryEntry::decode(buffer::ptr::const_iterator &it) {
  uint32_t query_type;
  denc(query_type, it);

  switch (query_type) {
  case OSD_PERF_QUERY_TYPE_CLIENT_ID:
    query = ClientIdOSDPerfMetricQuery();
    break;
  case OSD_PERF_QUERY_TYPE_RBD_IMAGE_ID:
    query = RBDImageIdOSDPerfMetricQuery();
    break;
  default:
    query = UnknownOSDPerfMetricQuery();
    break;
  }

  boost::apply_visitor(DecodeVisitor(it), query);
}

void OSDPerfMetricQueryEntry::dump(Formatter *f) const {
  boost::apply_visitor(DumpVisitor(f), query);
}

bool OSDPerfMetricQueryEntry::operator<(
    const OSDPerfMetricQueryEntry &other) const {
  if (get_query_type() != other.get_query_type()) {
    return get_query_type() < other.get_query_type();
  }

  switch (get_query_type()) {
  case OSD_PERF_QUERY_TYPE_CLIENT_ID:
    return boost::get<ClientIdOSDPerfMetricQuery>(query) <
       boost::get<ClientIdOSDPerfMetricQuery>(other.query);
  case OSD_PERF_QUERY_TYPE_RBD_IMAGE_ID:
    return boost::get<RBDImageIdOSDPerfMetricQuery>(query) <
        boost::get<RBDImageIdOSDPerfMetricQuery>(other.query);
  default:
    return boost::get<UnknownOSDPerfMetricQuery>(query) <
        boost::get<UnknownOSDPerfMetricQuery>(other.query);
  }
}

bool OSDPerfMetricQueryEntry::operator()(const MOSDOp *m,
                                         std::string *key) const {
  return boost::apply_visitor(MakeKeyVisitor(m, key), query);
}

std::ostream &operator<<(std::ostream &out,
                         const OSDPerfMetricQueryEntry &entry) {
  return boost::apply_visitor(PrintVisitor(out), entry.query);
}
