// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace migrate {

namespace at = argument_types;
namespace po = boost::program_options;

static int abort_migrate(librados::IoCtx& io_ctx, const std::string &image_name,
                         bool no_progress) {
  utils::ProgressContext pc("Abort image migrate", no_progress);
  int r = librbd::RBD().migrate_abort_with_progress(io_ctx, image_name.c_str(),
                                                    pc);
  if (r < 0) {
    pc.fail();
    std::cerr << "rbd: aborting migration failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  pc.finish();
  return 0;
}

static int do_migrate(librados::IoCtx& io_ctx, const std::string &image_name,
                      librados::IoCtx& dest_io_ctx,
                      const std::string &dest_image_name,
                      librbd::ImageOptions& opts, bool no_progress) {
  utils::ProgressContext pc("Image migrate", no_progress);
  int r = librbd::RBD().migrate_with_progress(io_ctx, image_name.c_str(),
                                              dest_io_ctx,
                                              dest_image_name.c_str(), opts,
                                              pc);
  if (r < 0) {
    pc.fail();
    std::cerr << "rbd: migrate failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  pc.finish();
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE);
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, true);
  at::add_no_progress_option(options);

  options->add_options()
    ("abort", po::bool_switch(),
     "cancel previously started but interrupted migration");
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    nullptr, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }
  io_ctx.set_osdmap_full_try();

  std::string dest_pool_name;
  std::string dest_image_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &dest_pool_name,
    &dest_image_name, nullptr, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_FULL, false);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  librados::IoCtx dest_io_ctx;
  if (!dest_pool_name.empty()) {
    r = utils::init_io_ctx(rados, dest_pool_name, &dest_io_ctx);
    if (r < 0) {
      return r;
    }
  }

  if (vm["abort"].as<bool>()) {
    r = abort_migrate(io_ctx, image_name, vm[at::NO_PROGRESS].as<bool>());
  } else {
    r = do_migrate(io_ctx, image_name,
                   dest_pool_name.empty() ? io_ctx : dest_io_ctx,
                   dest_image_name, opts, vm[at::NO_PROGRESS].as<bool>());
  }
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::Action action(
  {"migrate"}, {}, "Migrate src image to dest.", at::get_long_features_help(),
  &get_arguments, &execute);

} // namespace migrate
} // namespace action
} // namespace rbd
