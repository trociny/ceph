// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/param.h>
#include <errno.h>
#include <unistd.h>

#include "include/stringify.h"
#include "common/SubProcess.h"

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/scope_exit.hpp>
#include <boost/program_options.hpp>

#include <iostream>

namespace rbd {
namespace action {
namespace ggate {

namespace at = argument_types;
namespace po = boost::program_options;

static int call_ggate_cmd(const po::variables_map &vm,
                          const std::vector<const char*> &args)
{
  SubProcess process("rbd-ggate", SubProcess::KEEP, SubProcess::KEEP,
                     SubProcess::KEEP);

  if (vm.count("conf")) {
    process.add_cmd_arg("--conf");
    process.add_cmd_arg(vm["conf"].as<std::string>().c_str());
  }
  if (vm.count("cluster")) {
    process.add_cmd_arg("--cluster");
    process.add_cmd_arg(vm["cluster"].as<std::string>().c_str());
  }
  if (vm.count("id")) {
    process.add_cmd_arg("--id");
    process.add_cmd_arg(vm["id"].as<std::string>().c_str());
  }
  if (vm.count("name")) {
    process.add_cmd_arg("--name");
    process.add_cmd_arg(vm["name"].as<std::string>().c_str());
  }
  if (vm.count("mon_host")) {
    process.add_cmd_arg("--mon_host");
    process.add_cmd_arg(vm["mon_host"].as<std::string>().c_str());
  }
  if (vm.count("keyfile")) {
    process.add_cmd_arg("--keyfile");
    process.add_cmd_arg(vm["keyfile"].as<std::string>().c_str());
  }
  if (vm.count("keyring")) {
    process.add_cmd_arg("--keyring");
    process.add_cmd_arg(vm["keyring"].as<std::string>().c_str());
  }

  for (std::vector<const char*>::const_iterator p = args.begin();
       p != args.end(); ++p)
    process.add_cmd_arg(*p);

  if (process.spawn()) {
    std::cerr << "rbd: failed to run rbd-ggate: " << process.err() << std::endl;
    return -EINVAL;
  } else if (process.join()) {
    std::cerr << "rbd: rbd-ggate failed with error: " << process.err()
              << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_image_or_snap_spec(const po::variables_map &vm, std::string *spec) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  spec->append(pool_name);
  spec->append("/");
  spec->append(image_name);
  if (!snap_name.empty()) {
    spec->append("@");
    spec->append(snap_name);
  }

  return 0;
}

void get_device_specific_map_options(const std::string &help_suffix,
                                     po::options_description *options) {
  options->add_options()
    ("device", po::value<std::string>(),
     ("specify ggate device" + help_suffix).c_str());
}

int execute_show(const po::variables_map &vm)
{
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::vector<const char*> args;

  args.push_back("list");

  if (vm.count("format")) {
    args.push_back("--format");
    args.push_back(vm["format"].as<at::Format>().value.c_str());
  }
  if (vm["pretty-format"].as<bool>()) {
    args.push_back("--pretty-format");
  }

  return call_ggate_cmd(vm, args);
}

int execute_map(const po::variables_map &vm)
{
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::vector<const char*> args;

  args.push_back("map");
  std::string img;
  int r = get_image_or_snap_spec(vm, &img);
  if (r < 0) {
    return r;
  }
  args.push_back(img.c_str());

  if (vm["read-only"].as<bool>())
    args.push_back("--read-only");

  if (vm["exclusive"].as<bool>())
    args.push_back("--exclusive");

  if (vm.count("device")) {
    args.push_back("--device");
    args.push_back(vm["device"].as<std::string>().c_str());
  }

  return call_ggate_cmd(vm, args);
}

int execute_unmap(const po::variables_map &vm)
{
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  std::string image_name;
  if (device_name.empty()) {
    int r = get_image_or_snap_spec(vm, &image_name);
    if (r < 0) {
      return r;
    }
  }

  if (device_name.empty() && image_name.empty()) {
    std::cerr << "rbd: unmap requires either image name or device path"
              << std::endl;
    return -EINVAL;
  }

  std::vector<const char*> args;

  args.push_back("unmap");
  args.push_back(device_name.empty() ? image_name.c_str() :
                 device_name.c_str());

  return call_ggate_cmd(vm, args);
}

} // namespace ggate
} // namespace action
} // namespace rbd
