// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "acconfig.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rados.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/SubProcess.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>

#include <cstring>
#include <iostream>
#include <regex>
#include <unistd.h>

/*
 * This is a thin wrapper around a ublk control binary: all of the actual
 * librbd/librados I/O logic lives in the target process it execs (an
 * external runtime dependency -- same relationship "rbd device map -t nbd"
 * has with the nbd kernel module), not here. This code only translates
 * "rbd device {list,map,unmap}" into that binary's own {list,add,del}
 * invocations.
 *
 * Two independent implementations of that binary exist and are both
 * supported here: ublksrv's C++ "ublk"/"ublk.rbd" (the default), and
 * rublk's Rust "rublk" (opt in via RBD_UBLK=rublk in the environment --
 * see use_rublk() below). They're close enough in spirit -- both take
 * "add"/"del"/"list"/"recover" and print a broadly similar per-device dump
 * -- that most of this file's argument-building and output-parsing logic
 * is shared, but they differ in enough small, concrete ways that a few
 * spots have to branch explicitly on which one is in use:
 *  - "ublk add -t rbd ... -r 1" enables user-recovery via a "-r" flag that
 *    takes a value; rublk's "-r" is a bare boolean (clap) flag, and the
 *    target name is a subcommand ("rublk add rbd ...") rather than a "-t"
 *    argument to a generic "add".
 *  - "ublk list" writes pool/image/etc directly as fields of the "target"
 *    JSON blob, with an empty string/0 standing in for "unset"; rublk's
 *    generic libublk-rs architecture instead splits that into a "target"
 *    blob holding only generic fields (dev_size/name/type) and a separate
 *    "target_data" blob keyed by target name (here, "rbd") holding the
 *    per-target fields, representing "unset" as a real JSON null rather
 *    than an empty string/0.
 */

namespace rbd {
namespace action {
namespace ublk {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

struct MappedDevice {
  int dev_id = -1;
  int daemon_pid = -1;
  std::string state;
  std::string devpath;
  std::string pool_name;
  std::string nspace_name;
  std::string image_name;
  std::string snap_name;
  uint64_t snap_id = CEPH_NOSNAP;
};

struct UblkTarget {
  std::string pool;
  std::string nspace;
  std::string image;
  std::string snap;
  uint64_t snap_id = 0;
  bool has_snap_id = false;
};

/* RBD_UBLK selects which of the two "ublk"-shaped binaries this file
 * drives: unset/anything else means ublksrv's C++ "ublk" (the default,
 * matching how this integration originally shipped); "rublk" opts into
 * the newer Rust implementation instead. This is a *mode* switch, not a
 * path -- the binary is still looked up via PATH by SubProcess, same as
 * "ublk" always has been, so an operator wanting a non-default install
 * location handles that the same way as any other PATH-resolved tool
 * (adjusting PATH itself), not through this variable.
 */
bool use_rublk() {
  const char *v = getenv("RBD_UBLK");
  return v && std::string(v) == "rublk";
}

std::string ublk_cmd_name() {
  return use_rublk() ? "rublk" : "ublk";
}

// mirrors the fields ublk.rbd's init_tgt stashes via ublk_json_write_tgt_*:
// pool/image/etc are fields directly on the "target" JSON blob, alongside
// a "name" identifying the target type; "" and 0/false stand in for
// "unset" namespace/snap/snap_id.
bool decode_ublksrv_target(JSONObj *obj, UblkTarget *t) {
  std::string name;
  JSONDecoder::decode_json("name", name, obj);
  if (name != "rbd") {
    return false;
  }
  JSONDecoder::decode_json("pool", t->pool, obj);
  JSONDecoder::decode_json("namespace", t->nspace, obj);
  JSONDecoder::decode_json("image", t->image, obj);
  JSONDecoder::decode_json("snap", t->snap, obj);
  JSONDecoder::decode_json("snap_id", t->snap_id, obj);
  JSONDecoder::decode_json("has_snap_id", t->has_snap_id, obj);
  return true;
}

/* rublk's generic libublk-rs architecture keeps only dev_size/name/type on
 * the "target" blob; per-target fields live in a separate "target_data"
 * blob keyed by target name -- {"rbd": {"pool": ..., "image": ..., ...}}
 * -- rather than inline on "target" itself, and "unset" is a real JSON
 * null rather than an empty string/0. A null decodes, via the plain
 * string/uint64_t JSONDecoder paths, to the literal 4-character text
 * "null" (see JSONObj::init() in ceph_json.cc: non-string JSON values are
 * round-tripped back to their JSON text, and json_spirit renders null as
 * that text) rather than throwing -- so each field is checked against
 * that sentinel explicitly below instead of trusting decode_json's own
 * success/failure the way decode_ublksrv_target() above safely can for
 * ublksrv's own (never-null) encoding.
 */
bool decode_rublk_target(JSONObj *target_data_obj, UblkTarget *t) {
  JSONObj *rbd_obj = target_data_obj->find_obj("rbd");
  if (!rbd_obj) {
    return false;
  }

  JSONDecoder::decode_json("pool", t->pool, rbd_obj);
  JSONDecoder::decode_json("namespace", t->nspace, rbd_obj);
  JSONDecoder::decode_json("image", t->image, rbd_obj);
  JSONDecoder::decode_json("snap", t->snap, rbd_obj);
  if (t->nspace == "null") {
    t->nspace.clear();
  }
  if (t->snap == "null") {
    t->snap.clear();
  }

  std::string snap_id_str;
  JSONDecoder::decode_json("snap_id", snap_id_str, rbd_obj);
  t->has_snap_id = !snap_id_str.empty() && snap_id_str != "null";
  t->snap_id = t->has_snap_id ? strtoull(snap_id_str.c_str(), NULL, 10) : 0;

  return !t->pool.empty() && t->pool != "null" &&
         !t->image.empty() && t->image != "null";
}

/* ublk.rbd's own option parser only understands --conf/--id/--cluster (not
 * the full ceph_argparse vocabulary), so translate the subset of global
 * ceph args that matter for connecting to the right cluster. Anything else
 * (e.g. -m/--mon-host) has no ublk.rbd equivalent and is dropped.
 */
void translate_ceph_args(const std::vector<std::string> &ceph_global_init_args,
                          std::vector<std::string> *args) {
  for (size_t i = 0; i < ceph_global_init_args.size(); i++) {
    const std::string &arg = ceph_global_init_args[i];
    std::string val = (i + 1 < ceph_global_init_args.size()) ?
      ceph_global_init_args[i + 1] : std::string();

    if (arg == "-c" || arg == "--conf") {
      args->push_back("--conf");
      args->push_back(val);
      i++;
    } else if (arg == "-i" || arg == "--id") {
      args->push_back("--id");
      args->push_back(val);
      i++;
    } else if (arg == "-n" || arg == "--name") {
      if (boost::starts_with(val, "client.")) {
        val = val.substr(strlen("client."));
      }
      args->push_back("--id");
      args->push_back(val);
      i++;
    } else if (arg == "--cluster") {
      args->push_back("--cluster");
      args->push_back(val);
      i++;
    }
  }
}

int call_ublk_cmd(const std::vector<std::string> &args,
                  SubProcess::std_fd_op stdout_op,
                  std::string *output) {
  std::string cmd = ublk_cmd_name();
  SubProcess process(cmd.c_str(), SubProcess::CLOSE, stdout_op, SubProcess::KEEP);

  for (auto &arg : args) {
    process.add_cmd_arg(arg.c_str());
  }

  int r = process.spawn();
  if (r < 0) {
    std::cerr << "rbd: failed to run " << cmd << ": " << process.err()
              << std::endl;
    return r;
  }

  if (stdout_op == SubProcess::PIPE) {
    char buf[4096];
    ssize_t n;
    while ((n = read(process.get_stdout(), buf, sizeof(buf))) > 0) {
      output->append(buf, n);
    }
  }

  r = process.join();
  if (r != 0) {
    std::cerr << "rbd: " << cmd << " failed with error: " << process.err()
              << std::endl;
    return -EINVAL;
  }
  return 0;
}

/*
 * "ublk list"/"rublk list" both print one block per device, e.g. (ublk):
 *
 *   dev id 1: nr_hw_queues 1 queue_depth 128 block size 512 dev_capacity ...
 *           ...
 *           target {"cluster":"","pool":"rbd","image":"foo",...,"name":"rbd"}
 *
 * or (rublk):
 *
 *   dev id 1: nr_hw_queues 1 queue_depth 128 block size 512 dev_capacity ...
 *           max rq size ... daemon pid ... flags ... state LIVE
 *           ...
 *           target {"dev_size":...,"name":"rbd","type":0}
 *           target_data {"rbd":{"pool":"rbd","image":"foo",...}}
 *
 * The "target"/"target_data" lines only appear for devices whose target
 * actually wrote something via the respective JSON-persistence mechanism,
 * which is exactly the blob each rbd target's own init stashes -- see
 * decode_ublksrv_target()/decode_rublk_target() above for the two shapes.
 */
int list_ublk_rbd_devices(std::vector<MappedDevice> *devices) {
  std::string output;
  int r = call_ublk_cmd({"list"}, SubProcess::PIPE, &output);
  if (r < 0) {
    return r;
  }

  static const std::regex dev_id_re("dev id ([0-9]+):");
  std::vector<std::pair<int, size_t>> starts;
  for (auto it = std::sregex_iterator(output.begin(), output.end(), dev_id_re);
       it != std::sregex_iterator(); ++it) {
    starts.emplace_back(std::stoi((*it)[1]), (size_t)it->position());
  }

  static const std::regex target_re("target (\\{.*\\})");
  static const std::regex target_data_re("target_data (\\{.*\\})");
  // rublk's line additionally carries a "flags 0x.." token between the pid
  // and the state that ublk's own equivalent line doesn't; ".*" tolerates
  // it either way (regex "." doesn't cross the newline into another line).
  static const std::regex pid_re("daemon pid (-?[0-9]+).*state (\\w+)");
  bool rublk = use_rublk();
  for (size_t idx = 0; idx < starts.size(); idx++) {
    size_t block_start = starts[idx].second;
    size_t block_end = (idx + 1 < starts.size()) ? starts[idx + 1].second :
      output.size();
    std::string block = output.substr(block_start, block_end - block_start);

    UblkTarget t;
    bool matched;
    try {
      if (rublk) {
        std::smatch tm;
        if (!std::regex_search(block, tm, target_data_re)) {
          continue;
        }
        std::string json_str = tm[1].str();
        JSONParser p;
        matched = p.parse(json_str.c_str(), json_str.length()) &&
                  decode_rublk_target(&p, &t);
      } else {
        std::smatch tm;
        if (!std::regex_search(block, tm, target_re)) {
          continue;
        }
        std::string json_str = tm[1].str();
        JSONParser p;
        matched = p.parse(json_str.c_str(), json_str.length()) &&
                  decode_ublksrv_target(&p, &t);
      }
    } catch (const JSONDecoder::err&) {
      continue;
    }
    if (!matched) {
      continue;
    }

    MappedDevice d;

    // the target JSON can outlive a daemon that was killed rather than
    // cleanly unmapped (e.g. SIGKILL) -- skip those (DEAD, or FAIL_IO
    // without a cooperating daemon), but keep QUIESCED ones: that's the
    // state a device added with user-recovery enabled ("-r 1" for ublk,
    // "-r" for rublk) lands in when its daemon dies unexpectedly, and it's
    // recoverable via "rbd device recover" (see execute_recover()), so it
    // should stay visible for users to spot and recover by id rather than
    // being silently dropped from the list.
    std::smatch pm;
    if (!std::regex_search(block, pm, pid_re) ||
        (pm[2] != "LIVE" && pm[2] != "QUIESCED")) {
      continue;
    }
    d.daemon_pid = std::stoi(pm[1]);
    d.state = pm[2];

    d.dev_id = starts[idx].first;
    d.devpath = "/dev/ublkb" + std::to_string(d.dev_id);
    d.pool_name = t.pool;
    d.nspace_name = t.nspace;
    d.image_name = t.image;
    d.snap_name = t.snap;
    d.snap_id = t.has_snap_id ? t.snap_id : CEPH_NOSNAP;

    devices->push_back(d);
  }

  return 0;
}

int find_dev_id_by_devpath(const std::string &devpath, int *dev_id) {
  static const std::regex pattern("^/dev/ublkb([0-9]+)$");
  std::smatch m;
  if (!std::regex_match(devpath, m, pattern)) {
    return -EINVAL;
  }
  *dev_id = std::stoi(m[1]);
  return 0;
}

int find_mapped_dev_by_spec(const std::string &pool_name,
                            const std::string &nspace_name,
                            const std::string &image_name,
                            const std::string &snap_name,
                            uint64_t snap_id, int *dev_id) {
  std::vector<MappedDevice> devices;
  int r = list_ublk_rbd_devices(&devices);
  if (r < 0) {
    return r;
  }

  for (auto &d : devices) {
    if (d.pool_name == pool_name && d.nspace_name == nspace_name &&
        d.image_name == image_name && d.snap_name == snap_name &&
        d.snap_id == snap_id) {
      *dev_id = d.dev_id;
      return 0;
    }
  }
  return -ENOENT;
}

} // anonymous namespace

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  std::vector<MappedDevice> devices;
  r = list_ublk_rbd_devices(&devices);
  if (r < 0) {
    std::cerr << "rbd: device list failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  bool should_print = false;
  TextTable tbl;
  if (formatter) {
    formatter->open_array_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("pool", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("namespace", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("snap", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("daemon_pid", TextTable::LEFT, TextTable::LEFT);
  }

  for (auto &d : devices) {
    std::string snap = (d.snap_id != CEPH_NOSNAP ?
        "@" + std::to_string(d.snap_id) : d.snap_name);
    if (formatter) {
      formatter->open_object_section("device");
      formatter->dump_int("id", d.dev_id);
      formatter->dump_string("pool", d.pool_name);
      formatter->dump_string("namespace", d.nspace_name);
      formatter->dump_string("image", d.image_name);
      formatter->dump_string("snap", snap);
      formatter->dump_string("device", d.devpath);
      formatter->dump_int("daemon_pid", d.daemon_pid);
      formatter->dump_string("state", d.state);
      formatter->close_section();
    } else {
      should_print = true;
      // a QUIESCED device's daemon is dead (only recoverable via "rbd
      // device recover"), so blank out the pid rather than showing a pid
      // that no longer refers to a running process.
      std::string pid = (d.state == "LIVE") ?
        std::to_string(d.daemon_pid) : "-";
      tbl << d.dev_id << d.pool_name << d.nspace_name << d.image_name
          << (snap.empty() ? "-" : snap) << d.devpath << pid
          << TextTable::endrow;
    }
  }

  if (formatter) {
    formatter->close_section(); // devices
    formatter->flush(std::cout);
  } else if (should_print) {
    std::cout << tbl;
  }
  return 0;
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string nspace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &nspace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }
  if (pool_name.empty()) {
    // rbd_default_pool is a mon config-store value: a freshly-started rbd
    // process doesn't see config overrides until it actually connects.
    librados::Rados rados;
    r = utils::init_rados(&rados);
    if (r < 0) {
      return r;
    }
  }
  utils::normalize_pool_name(&pool_name);

  // enable user-recovery so a dead daemon's device can be reattached via
  // "rbd device recover" instead of having to be re-mapped. "-t rbd"/"-r 1"
  // (ublk) vs the "rbd" subcommand/bare "-r" (rublk) is the one place the
  // two CLIs actually diverge in shape rather than just naming: ublk's
  // target type is a "-t" argument to a generic "add" and its "-r" takes a
  // 0/1 value, while rublk's target type is itself the subcommand and its
  // "-r"/"--user-recovery" is a bare boolean (clap) flag that a stray "1"
  // argument after it would trip over as unexpected.
  std::vector<std::string> args = use_rublk() ?
    std::vector<std::string>{"add", "rbd",
                             "--pool", pool_name,
                             "--image", image_name,
                             "-r"} :
    std::vector<std::string>{"add", "-t", "rbd",
                             "--pool", pool_name,
                             "--image", image_name,
                             "-r", "1"};
  if (!nspace_name.empty()) {
    args.push_back("--namespace");
    args.push_back(nspace_name);
  }
  if (vm.count(at::SNAPSHOT_ID)) {
    args.push_back("--snap-id");
    args.push_back(std::to_string(vm[at::SNAPSHOT_ID].as<uint64_t>()));
  } else if (!snap_name.empty()) {
    args.push_back("--snap");
    args.push_back(snap_name);
  }

  if (vm["read-only"].as<bool>()) {
    args.push_back("--read-only");
  }
  if (vm["exclusive"].as<bool>()) {
    args.push_back("--exclusive");
  }
  if (vm["quiesce"].as<bool>()) {
    // ublk.rbd's own hook-path option is named "--rbd-quiesce-hook" (not
    // "--quiesce-hook") to avoid confusion with ublk's unrelated QUIESCED
    // device state. Default to the same hook rbd-nbd uses -- its
    // <devpath> <quiesce|unquiesce> protocol is device-type-agnostic, so
    // the existing script works unchanged for a ublk device path too.
    args.push_back("--rbd-quiesce");
    args.push_back("--rbd-quiesce-hook");
    args.push_back(vm.count("quiesce-hook") ?
      vm["quiesce-hook"].as<std::string>() :
      CMAKE_INSTALL_LIBEXECDIR "/rbd-nbd/rbd-nbd_quiesce");
  }

  translate_ceph_args(ceph_global_init_args, &args);

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  std::string output;
  r = call_ublk_cmd(args, SubProcess::PIPE, &output);
  if (r < 0) {
    return r;
  }

  std::smatch m;
  static const std::regex dev_id_re("dev id ([0-9]+):");
  if (!std::regex_search(output, m, dev_id_re)) {
    std::cerr << "rbd: map succeeded but could not determine device id"
              << std::endl;
    std::cout << output;
    return 0;
  }

  std::cout << "/dev/ublkb" << m[1].str() << std::endl;
  return 0;
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  int dev_id = -1;
  if (!device_name.empty()) {
    int r = find_dev_id_by_devpath(device_name, &dev_id);
    if (r < 0) {
      std::cerr << "rbd: invalid device path '" << device_name << "'"
                << std::endl;
      return r;
    }
  } else {
    size_t arg_index = 0;
    std::string pool_name;
    std::string nspace_name;
    std::string image_name;
    std::string snap_name;
    int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &nspace_name,
      &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
      utils::SPEC_VALIDATION_NONE);
    if (r < 0) {
      return r;
    }
    if (pool_name.empty()) {
      librados::Rados rados;
      r = utils::init_rados(&rados);
      if (r < 0) {
        return r;
      }
    }
    utils::normalize_pool_name(&pool_name);

    uint64_t snap_id = CEPH_NOSNAP;
    if (vm.count(at::SNAPSHOT_ID)) {
      snap_id = vm[at::SNAPSHOT_ID].as<uint64_t>();
    }

    r = find_mapped_dev_by_spec(pool_name, nspace_name, image_name, snap_name,
                                snap_id, &dev_id);
    if (r < 0) {
      std::cerr << "rbd: " << pool_name << "/" << image_name
                << " is not mapped" << std::endl;
      return r;
    }
  }

  std::string output;
  return call_ublk_cmd({"del", "-n", std::to_string(dev_id)},
                       SubProcess::KEEP, &output);
}

int execute_attach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::cerr << "rbd: ublk device does not support attach" << std::endl;
  return -EOPNOTSUPP;
}

int execute_detach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::cerr << "rbd: ublk device does not support detach" << std::endl;
  return -EOPNOTSUPP;
}

int execute_recover(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  std::string dev_id_str = utils::get_positional_argument(vm, 0);
  if (dev_id_str.empty() ||
      dev_id_str.find_first_not_of("0123456789") != std::string::npos) {
    std::cerr << "rbd: recover requires a device id" << std::endl;
    return -EINVAL;
  }

  std::string output;
  return call_ublk_cmd({"recover", "-n", dev_id_str}, SubProcess::KEEP,
                       &output);
}

} // namespace ublk
} // namespace action
} // namespace rbd
