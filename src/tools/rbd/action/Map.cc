// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/krbd.h"
#include "include/stringify.h"
#include "include/uuid.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/strtol.h"
#include "common/Formatter.h"
#include "msg/msg_types.h"
#include "global/global_context.h"
#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/scope_exit.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {

namespace at = argument_types;
namespace po = boost::program_options;

#define DECLARE_DEVICE_FUNCTIONS(ns)                                            \
  namespace ns {                                                                \
  void get_device_specific_map_options(const std::string &help_suffix,          \
                                       po::options_description *options);       \
  void get_device_specific_unmap_options(const std::string &help_suffix,        \
                                       po::options_description *options);       \
  int execute_show(const po::variables_map &vm);                                \
  int execute_map(const po::variables_map &vm);                                 \
  int execute_unmap(const po::variables_map &vm);                               \
}

DECLARE_DEVICE_FUNCTIONS(ggate);
DECLARE_DEVICE_FUNCTIONS(kernel);
DECLARE_DEVICE_FUNCTIONS(nbd);

namespace map {

namespace {

enum device_type_t {
  DEVICE_TYPE_GGATE,
  DEVICE_TYPE_KERNEL,
  DEVICE_TYPE_NBD,
};

struct DeviceType {};

void validate(boost::any& v, const std::vector<std::string>& values,
              DeviceType *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "ggate") {
    v = boost::any(DEVICE_TYPE_GGATE);
  } else if (s == "kernel") {
    v = boost::any(DEVICE_TYPE_KERNEL);
  } else if (s == "nbd") {
    v = boost::any(DEVICE_TYPE_NBD);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void add_device_type_option(po::options_description *options) {
  options->add_options()
    ("device-type,t", po::value<DeviceType>(),
     "device type [ggate, kernel (default), nbd]");
}

device_type_t get_device_type(const po::variables_map &vm) {
  if (vm.count("device-type")) {
    return vm["device-type"].as<device_type_t>();
  }
  return DEVICE_TYPE_KERNEL;
}

} // anonymous namespace

void get_show_arguments(po::options_description *positional,
                        po::options_description *options) {
  add_device_type_option(options);
  at::add_format_options(options);
}

int execute_show(const po::variables_map &vm) {
  switch (get_device_type(vm)) {
  case DEVICE_TYPE_GGATE:
    return ggate::execute_show(vm);
  case DEVICE_TYPE_KERNEL:
    return kernel::execute_show(vm);
  case DEVICE_TYPE_NBD:
    return nbd::execute_show(vm);
  default:
    assert(0);
    return -EINVAL;
  }
}

void get_map_arguments(po::options_description *positional,
                       po::options_description *options) {
  add_device_type_option(options);
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("read-only", po::bool_switch(), "map read-only")
    ("exclusive", po::bool_switch(), "disable automatic exclusive lock transitions");

  kernel::get_device_specific_map_options(" *", options);
  nbd::get_device_specific_map_options(" +", options);
  ggate::get_device_specific_map_options(" #", options);
}

int execute_map(const po::variables_map &vm) {
  switch (get_device_type(vm)) {
  case DEVICE_TYPE_GGATE:
    return ggate::execute_map(vm);
  case DEVICE_TYPE_KERNEL:
    return kernel::execute_map(vm);
  case DEVICE_TYPE_NBD:
    return nbd::execute_map(vm);
  default:
    assert(0);
    return -EINVAL;
  }
}

void get_unmap_arguments(po::options_description *positional,
                   po::options_description *options) {
  add_device_type_option(options);
  positional->add_options()
    ("image-or-snap-or-device-spec",
     "image, snapshot, or device specification\n"
     "[<pool-name>/]<image-name>[@<snapshot-name>] or <device-path>");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_NONE);

  kernel::get_device_specific_unmap_options(" *", options);
}

int execute_unmap(const po::variables_map &vm) {
  switch (get_device_type(vm)) {
  case DEVICE_TYPE_GGATE:
    return ggate::execute_unmap(vm);
  case DEVICE_TYPE_KERNEL:
    return kernel::execute_unmap(vm);
  case DEVICE_TYPE_NBD:
    return nbd::execute_unmap(vm);
  default:
    assert(0);
    return -EINVAL;
  }
}

Shell::SwitchArguments switched_arguments({"read-only", "exclusive"});
Shell::Action action_show(
  {"showmapped"}, {}, "Show mapped rbd images.", "",
  &get_show_arguments, &execute_show);

Shell::Action action_map(
  {"map"}, {}, "Map an image to a block device.",
  "* kernel device specific option\n"
  "+ nbd device specific option\n"
  "# ggate device specific option\n",
  &get_map_arguments, &execute_map);

Shell::Action action_unmap(
  {"unmap"}, {}, "Unmap a rbd device.",
  "* kernel device specific option\n",
  &get_unmap_arguments, &execute_unmap);

} // namespace map
} // namespace action
} // namespace rbd
