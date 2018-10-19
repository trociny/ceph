// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "RBDPerfCounters.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.rbd_perf_counters " << __func__ << " "

int RBDPerfCounters::get_counter(int64_t pool_id, const std::string &image_id,
                                 const std::string &name, PerfCounterType *type,
                                 PerfCounterInstance *instance) const {
  RWLock::RLocker locker(lock);

  dout(20) << pool_id << " " << image_id << " " << name << dendl;

  if (types.count(name) == 0) {
    dout(20) << "invalid counter name: " << name << dendl;
    return -EINVAL;
  }

  std::string path = stringify(pool_id) + "." + image_id + "_" + name;

  if (!instances.count(path)) {
    dout(20) << "no data for " << path << dendl;
    return -ENOENT;
  }

  *type = types.at(name);
  *instance = instances.at(path);

  return 0;
}

void RBDPerfCounters::update(const OSDPerfMetricData &data) {
  if (data.empty()) {
    return;
  }

  const auto now = ceph_clock_now();
  RWLock::WLocker locker(lock);

  std::map<std::string, std::map<std::string, uint64_t>> pdata;
  for (auto &it : data) {
    auto &key = it.first;
    auto &val = it.second;
    auto pos = key.find('_');
    if (pos == std::string::npos || pos == 0 || pos >= key.size() - 2) {
      continue;
    }

    auto path = std::string(key, 0, pos);
    auto type_name = std::string(key, pos + 1);
    pdata[path][type_name] = val;
  }

  for (auto &it : pdata) {
    auto &path = it.first;
    auto &vals = it.second;

    if (!vals.count("op")) {
      // should not happen
      continue;
    }

    auto op_path = path + "_op";
    auto inb_path = path + "_inb";
    auto outb_path = path + "_outb";
    auto lat_path = path + "_lat";
    uint64_t op = vals["op"];
    uint64_t inb = vals["inb"];
    uint64_t outb = vals["outb"];
    utime_t lat = utime_t(0, vals["lat"]);
    if (!instances.count(op_path)) {
      instances.insert({op_path, PerfCounterInstance(types["op"].type)});
      instances.insert({inb_path, PerfCounterInstance(types["inb"].type)});
      instances.insert({outb_path, PerfCounterInstance(types["outb"].type)});
      instances.insert({lat_path, PerfCounterInstance(types["lat"].type)});
      dout(20) << "new instance " << path << dendl;
    } else {
      op += instances.at(op_path).get_latest_data().v;
      inb += instances.at(inb_path).get_latest_data_avg().s;
      outb += instances.at(outb_path).get_latest_data_avg().s;
      lat += instances.at(lat_path).get_latest_data_avg().s;
    }

    dout(20) << path << dendl;

    instances.at(op_path).push(now, op);
    instances.at(inb_path).push_avg(now, inb, op);
    instances.at(outb_path).push_avg(now, outb, op);
    instances.at(lat_path).push_avg(now, lat, op);
  }
}
