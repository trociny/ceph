// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_PERF_COUNTERS_H_
#define RBD_PERF_COUNTERS_H_

#include <map>

#include "common/Mutex.h"
#include "common/RWLock.h"

// For PerfCounterType
#include "messages/MMgrReport.h"

#include "mgr/OSDPerfMetricReport.h"
#include "mgr/PerfCounterInstance.h"


class RBDPerfCounters
{
public:
  std::map<std::string, PerfCounterType> get_types() const {
    return types;
  }
  int get_counter(int64_t pool_id, const std::string &image_id,
                  const std::string &name, PerfCounterType *type,
                  PerfCounterInstance *instance) const;

  void update(const OSDPerfMetricData &data);

private:
  mutable RWLock lock = {"RBDPerfCounters", true, true, true};
  std::map<std::string, PerfCounterType> types = {
    {"op", {"op", "operations count", "", PERFCOUNTER_U64, UNIT_NONE}},
    {"inb", {"inb", "input bytes", "", (perfcounter_type_d)(PERFCOUNTER_U64 | PERFCOUNTER_LONGRUNAVG), UNIT_BYTES}},
    {"outb", {"outb", "output bytes", "", (perfcounter_type_d)(PERFCOUNTER_U64 | PERFCOUNTER_LONGRUNAVG), UNIT_BYTES}},
    {"lat", {"lat", "latency", "", (perfcounter_type_d)(PERFCOUNTER_TIME | PERFCOUNTER_LONGRUNAVG), UNIT_BYTES}},
  };
  std::map<std::string, PerfCounterInstance> instances;
};

#endif // RBD_PERF_COUNTERS_H_
