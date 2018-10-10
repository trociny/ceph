// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DYNAMIC_PERF_STATS_H
#define DYNAMIC_PERF_STATS_H

#include "mgr/OSDPerfMetric.h"
#include "mgr/OSDPerfMetricReport.h"

class DynamicPerfStats {
public:
  DynamicPerfStats() {
  }

  DynamicPerfStats(const std::list<OSDPerfMetricQueryEntry> &queries) {
    for (auto &query : queries) {
      data[query];
    }
  }

  void set_queries(const std::list<OSDPerfMetricQueryEntry> &queries) {
    std::map<OSDPerfMetricQueryEntry,
             std::map<std::string, PerfCounters>> new_data;
    for (auto &query : queries) {
      new_data[query] = std::move(data[query]);
    }
    data = std::move(new_data);
  }

  bool is_enabled() {
    return !data.empty();
  }

  void add(const MOSDOp *m, uint64_t inb, uint64_t outb, const utime_t &latency) {
    for (auto &it : data) {
      auto &query = it.first;
      std::string key;
      if (query(m, &key)) {
        it.second[key] += {1, inb, outb, latency.to_nsec()};
      }
    }
  }

  void add_to_report(OSDPerfMetricReport *report) {
    for (auto &it : data) {
      auto &query = it.first;
      auto &report_data = report->data[query];
      for (auto &it_counters : it.second) {
        report_data[it_counters.first + "_op"] += it_counters.second.count;
        report_data[it_counters.first + "_inb"] += it_counters.second.inb;
        report_data[it_counters.first + "_outb"] += it_counters.second.outb;
        report_data[it_counters.first + "_lat"] += it_counters.second.latency;
      }
    }
  }

private:
  struct PerfCounters {
    uint64_t count = 0;
    uint64_t inb = 0;
    uint64_t outb = 0;
    uint64_t latency = 0;

    PerfCounters() {
    }

    PerfCounters(uint64_t count, uint64_t inb, uint64_t outb, uint64_t latency)
      : count(count), inb(inb), outb(outb), latency(latency) {
    }

    PerfCounters &operator+=(const PerfCounters &pc) {
      count += pc.count;
      inb += pc.inb;
      outb += pc.outb;
      latency += latency;
      return *this;
    }
  };

  std::map<OSDPerfMetricQueryEntry, std::map<std::string, PerfCounters>> data;
};

#endif // DYNAMIC_PERF_STATS_H
