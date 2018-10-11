// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef OSD_PERF_METRIC_REPORT_H_
#define OSD_PERF_METRIC_REPORT_H_
#include "include/denc.h"
#include "mgr/OSDPerfMetric.h"

struct OSDPerfMetricReport
{
  std::map<OSDPerfMetricQueryEntry, OSDPerfMetricData> data;

  DENC(OSDPerfMetricReport, v, p) {
      DENC_START(1, 1, p);
      denc(v.data, p);
      DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricReport)
#endif // OSD_PERF_METRIC_REPORT_H_
