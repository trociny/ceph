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

static int do_migrate(librbd::Image &src, librados::IoCtx& dest_pp,
                      const std::string &destname, librbd::ImageOptions& opts,
                      bool no_progress)
{
  utils::ProgressContext pc("Image migrate", no_progress);
  int r = src.migrate_with_progress(dest_pp, destname.c_str(), opts, pc);
  if (r < 0) {
    pc.fail();
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
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    nullptr, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string dst_pool_name;
  std::string dst_image_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &dst_pool_name, &dst_image_name,
    nullptr, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL, false);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  librados::IoCtx dst_io_ctx;

  if (!dst_pool_name.empty()) {
    r = utils::init_io_ctx(rados, dst_pool_name, &dst_io_ctx);
    if (r < 0) {
      return r;
    }
  }

  r = do_migrate(image, dst_pool_name.empty() ? io_ctx : dst_io_ctx,
                 dst_image_name.c_str(), opts, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: migrate failed: " << cpp_strerror(r) << std::endl;
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
