// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "global/global_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>

namespace rbd {
namespace action {
namespace mirror_image {

namespace at = argument_types;
namespace po = boost::program_options;


void get_arguments(po::options_description *positional,
                           po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_arguments_disable(po::options_description *positional,
                           po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(), "disable even if not primary");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_enable_disable(const po::variables_map &vm, bool enable,
                           bool force) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = enable ? image.mirror_image_enable() : image.mirror_image_disable(force);
  if (r < 0) {
    return r;
  }

  std::cout << (enable ? "Mirroring enabled" : "Mirroring disabled")
    << std::endl;

  return 0;
}

int execute_disable(const po::variables_map &vm) {
  return execute_enable_disable(vm, false, vm["force"].as<bool>());
}

int execute_enable(const po::variables_map &vm) {
  return execute_enable_disable(vm, true, false);
}

void get_arguments_promote(po::options_description *positional,
                           po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(), "promote even if not cleanly demoted by remote cluster");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_promote(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  bool force = vm["force"].as<bool>();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = image.mirror_image_promote(force);
  if (r < 0) {
    std::cerr << "rbd: error promoting image to primary" << std::endl;
    return r;
  }

  std::cout << "" << std::endl;
  return 0;
}

int execute_demote(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = image.mirror_image_demote();
  if (r < 0) {
    std::cerr << "rbd: error demoting image to secondary" << std::endl;
    return r;
  }

  std::cout << "Image demoted to secondary" << std::endl;
  return 0;
}

int execute_resync(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = image.mirror_image_resync();
  if (r < 0) {
    std::cerr << "rbd: error flagging image resync" << std::endl;
    return r;
  }

  std::cout << "Flagged image for resync from primary" << std::endl;
  return 0;
}

int execute_status(const po::variables_map &vm) {
  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  std::string image_id = utils::image_id(image);

  uint64_t instance_id;
  cls::rbd::MirrorImageStatus status;
  r = librbd::cls_client::mirror_image_status_get(&io_ctx, image_id,
						  &instance_id, &status);
  if (r < 0) {
    std::cerr << "rbd: failed to get status for image " << image_name << ": "
	      << cpp_strerror(r) << std::endl;
    return r;
  }

  std::list<obj_watch_t> watchers;
  r = io_ctx.list_watchers(RBD_MIRRORING_STATUS, &watchers);
  if (r < 0) {
    std::cerr << "rbd: failed to get " << RBD_MIRRORING_STATUS << " watchers: "
	      << cpp_strerror(r) << std::endl;
  }

  bool found(false);
  for (auto i = watchers.begin(); i != watchers.end(); i++) {
    if (static_cast<uint64_t>(i->watcher_id) == instance_id) {
      found = true;
    }
  }
  if (!found) {
    std::cerr << "rbd: " << RBD_MIRRORING_STATUS
	      << " not watched by claimed owner (client." << instance_id
	      << "), expect outdated status" << std::endl;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("image");
    formatter->dump_string("image_name", image_name);
    formatter->dump_string("image_id", image_id);
    formatter->dump_stream("state") << status.state;
    formatter->close_section();
  } else {
    std::cout << image_name << "(" << image_id << "): " << status.state
	      << std::endl;
  }

  return 0;
}

Shell::Action action_enable(
  {"mirror", "image", "enable"}, {},
  "Enable RBD mirroring for an image.", "",
  &get_arguments, &execute_enable);
Shell::Action action_disable(
  {"mirror", "image", "disable"}, {},
  "Disable RBD mirroring for an image.", "",
  &get_arguments_disable, &execute_disable);
Shell::Action action_promote(
  {"mirror", "image", "promote"}, {},
  "Promote an image to primary for RBD mirroring.", "",
  &get_arguments_promote, &execute_promote);
Shell::Action action_demote(
  {"mirror", "image", "demote"}, {},
  "Demote an image to secondary for RBD mirroring.", "",
  &get_arguments, &execute_demote);
Shell::Action action_resync(
  {"mirror", "image", "resync"}, {},
  "Force resync to primary image for RBD mirroring.", "",
  &get_arguments, &execute_resync);
Shell::Action action_status(
  {"mirror", "image", "status"}, {},
  "Show RDB mirroring status for image.", "",
  &get_arguments, &execute_status);

} // namespace mirror_image
} // namespace action
} // namespace rbd
