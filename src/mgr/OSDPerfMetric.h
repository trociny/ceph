// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_H_
#define OSD_PERF_METRIC_H_

#include "include/denc.h"
#include "include/stringify.h"
#include "messages/MOSDOp.h"

#include <boost/algorithm/string/predicate.hpp>

#include <functional>
#include <ostream>

namespace ceph {
class Formatter;
}

typedef int OSDPerfMetricQueryID;

typedef std::map<std::string, uint64_t> OSDPerfMetricData;
typedef std::function<void(const std::string &daemon,
                           const OSDPerfMetricData &data)> OSDPerfMetricHandler;

enum OSDPerfMetricQueryType {
  OSD_PERF_QUERY_TYPE_CLIENT_ID = 0,
  OSD_PERF_QUERY_TYPE_RBD_IMAGE_ID = 1,
};

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQueryType &type);

struct ClientIdOSDPerfMetricQuery {
  static const OSDPerfMetricQueryType TYPE = OSD_PERF_QUERY_TYPE_CLIENT_ID;

  bool operator<(const ClientIdOSDPerfMetricQuery &other) const {
    return false;
  }

  bool operator()(const MOSDOp *m, std::string *key) const {
    *key = stringify(m->get_reqid().name);
    return true;
  }

  DENC(ClientIdOSDPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
  }
};
WRITE_CLASS_DENC(ClientIdOSDPerfMetricQuery)

struct RBDImageIdOSDPerfMetricQuery {
  static const OSDPerfMetricQueryType TYPE = OSD_PERF_QUERY_TYPE_RBD_IMAGE_ID;

  bool operator<(const RBDImageIdOSDPerfMetricQuery &other) const {
    return false;
  }

  bool operator()(const MOSDOp *m, std::string *key) const {
    if (!boost::starts_with(m->get_oid().name, "rbd_data.")) {
      return false;
    }

    auto count = m->get_oid().name.find('.', 9);
    if (count == std::string::npos) {
      return false;
    }

    *key = stringify(m->get_spg().pool()) + "." +
        stringify(m->get_oid().name.substr(9, count - 9));
    return true;
  }

  DENC(RBDImageIdOSDPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
  }
};
WRITE_CLASS_DENC(RBDImageIdOSDPerfMetricQuery)

struct UnknownOSDPerfMetricQuery {
  static const OSDPerfMetricQueryType TYPE = static_cast<OSDPerfMetricQueryType>(-1);

  bool operator<(const UnknownOSDPerfMetricQuery &other) const {
    return false;
  }
  bool operator()(const MOSDOp *m, std::string *key) const {
    return false;
  }

  DENC(UnknownOSDPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
  }
};
WRITE_CLASS_DENC(UnknownOSDPerfMetricQuery)

typedef boost::mpl::vector<ClientIdOSDPerfMetricQuery,
                           RBDImageIdOSDPerfMetricQuery,
                           UnknownOSDPerfMetricQuery> OSDPerfMetricQueryVector;
typedef boost::make_variant_over<OSDPerfMetricQueryVector>::type OSDPerfMetricQuery;

struct OSDPerfMetricQueryEntry {
  OSDPerfMetricQueryEntry() : query(UnknownOSDPerfMetricQuery()){
  }
  OSDPerfMetricQueryEntry(const OSDPerfMetricQuery &query) : query(query) {
  }

  OSDPerfMetricQuery query;

  bool operator<(const OSDPerfMetricQueryEntry &other) const;
  bool operator()(const MOSDOp *m, std::string *key) const;

  OSDPerfMetricQueryType get_query_type() const;

  void bound_encode(size_t &p) const;
  void encode(buffer::list::contiguous_appender& p) const;
  void decode(buffer::ptr::const_iterator &p);
  void dump(Formatter *f) const;
};
WRITE_CLASS_DENC(OSDPerfMetricQueryEntry)

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQueryEntry &q);

#endif // OSD_PERF_METRIC_H_

