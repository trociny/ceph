// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/SubProcess.h"
#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/scope_exit.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace nbd {

namespace at = argument_types;
namespace po = boost::program_options;

static int call_nbd_cmd(const po::variables_map &vm,
                        const std::vector<const char*> &args)
{
  char exe_path[PATH_MAX];
  ssize_t exe_path_bytes = readlink("/proc/self/exe", exe_path,
				    sizeof(exe_path) - 1);
  if (exe_path_bytes < 0) {
    strcpy(exe_path, "rbd-nbd");
  } else {
    if (snprintf(exe_path + exe_path_bytes,
                 sizeof(exe_path) - exe_path_bytes,
                 "-nbd") < 0) {
      return -EOVERFLOW;
    }
  }

  SubProcess process(exe_path, SubProcess::KEEP, SubProcess::KEEP, SubProcess::KEEP);

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
    std::cerr << "rbd: failed to run rbd-nbd: " << process.err() << std::endl;
    return -EINVAL;
  } else if (process.join()) {
    std::cerr << "rbd: rbd-nbd failed with error: " << process.err() << std::endl;
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
     ("specify nbd device" + help_suffix).c_str())
    ("nbds_max", po::value<std::string>(),
     ("override module param nbds_max" + help_suffix).c_str())
    ("max_part", po::value<std::string>(),
     ("override module param max_part" + help_suffix).c_str())
    ("timeout", po::value<std::string>(),
     ("set nbd request timeout (seconds)" + help_suffix).c_str());
}

void get_show_arguments(po::options_description *positional,
                        po::options_description *options)
{ }

int execute_list(const po::variables_map &vm)
{
#if defined(__FreeBSD__)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::vector<const char*> args;

  args.push_back("list-mapped");

  return call_nbd_cmd(vm, args);
}

int execute_list_deprecated(const po::variables_map &vm)
{
  std::cerr << "rbd: 'nbd list' command is deprecated, "
            << "use 'device list -t nbd' instead" << std::endl;
  return execute_list(vm);
}

void get_map_arguments(po::options_description *positional,
                       po::options_description *options)
{
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("read-only", po::bool_switch(), "map read-only")
    ("exclusive", po::bool_switch(), "forbid writes by other clients");
  get_device_specific_map_options("", options);
}

int execute_map(const po::variables_map &vm)
{
#if defined(__FreeBSD__)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
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
  if (vm.count("nbds_max")) {
    args.push_back("--nbds_max");
    args.push_back(vm["nbds_max"].as<std::string>().c_str());
  }
  if (vm.count("max_part")) {
    args.push_back("--max_part");
    args.push_back(vm["max_part"].as<std::string>().c_str());
  }
  if (vm.count("timeout")) {
    args.push_back("--timeout");
    args.push_back(vm["timeout"].as<std::string>().c_str());
  }

  return call_nbd_cmd(vm, args);
}

int execute_map_deprecated(const po::variables_map &vm)
{
  std::cerr << "rbd: 'nbd map' command is deprecated, "
            << "use 'device map -t nbd' instead" << std::endl;
  return execute_map(vm);
}

void get_unmap_arguments(po::options_description *positional,
                   po::options_description *options)
{
  positional->add_options()
    ("image-or-snap-or-device-spec",
     "image, snapshot, or device specification\n"
     "[<pool-name>/]<image-name>[@<snapshot-name>] or <device-path>");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_unmap(const po::variables_map &vm)
{
#if defined(__FreeBSD__)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
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

  return call_nbd_cmd(vm, args);
}

int execute_unmap_deprecated(const po::variables_map &vm)
{
  std::cerr << "rbd: 'nbd unmap' command is deprecated, "
            << "use 'device unmap -t nbd' instead" << std::endl;
  return execute_unmap(vm);
}

Shell::SwitchArguments switched_arguments({"read-only"});

Shell::Action action_show(
  {"nbd", "list"}, {"nbd", "ls"}, "List the nbd devices already used.", "",
  &get_show_arguments, &execute_list_deprecated, false);

Shell::Action action_map(
  {"nbd", "map"}, {}, "Map image to a nbd device.", "",
  &get_map_arguments, &execute_map_deprecated, false);

Shell::Action action_unmap(
  {"nbd", "unmap"}, {}, "Unmap a nbd device.", "",
  &get_unmap_arguments, &execute_unmap_deprecated, false);

} // namespace nbd
} // namespace action
} // namespace rbd
