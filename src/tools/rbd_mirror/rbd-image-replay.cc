// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "ImageReplayer.h"

#include <string>
#include <vector>

rbd::mirror::ImageReplayer *replayer = nullptr;

void usage() {
  std::cout << "usage: rbd-image-replay [options...] <local-pool> <remote-pool> <image>" << std::endl;
  std::cout << std::endl;
  std::cout << "  local-pool    local (secondary, destination) pool" << std::endl;
  std::cout << "  remote-pool   remote (primary, source) pool" << std::endl;
  std::cout << "  image         image to replay (mirror)" << std::endl;
  std::cout << std::endl;
  std::cout << "options:\n";
  std::cout << "  -m monaddress[:port]      connect to specified monitor\n";
  std::cout << "  --keyring=<path>          path to keyring for local cluster\n";
  std::cout << "  --log-file=<logfile>      file to log debug output\n";
  std::cout << "  --debug-rbd-mirror=<log-level>/<memory-level>  set rbd-mirror debug level\n";
  generic_server_usage();
}

static atomic_t g_stopping;

static void handle_signal(int signum)
{
  g_stopping.set(1);
}

int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
	      CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  for (auto i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return EXIT_SUCCESS;
    }
  }

  if (args.size() < 3) {
    usage();
    return EXIT_FAILURE;
  }

  std::string local_pool_name = args[0];
  std::string remote_pool_name = args[1];
  std::string image_name = args[2];

  if (g_conf->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  g_ceph_context->enable_perf_counter();

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  rbd::mirror::RadosRef local(new librados::Rados());
  rbd::mirror::RadosRef remote(new librados::Rados());

  int r = local->init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    goto cleanup;
  }

  r = local->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    goto cleanup;
  }

  r = remote->init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    goto cleanup;
  }

  r = remote->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    goto cleanup;
  }

  replayer = new rbd::mirror::ImageReplayer(local, remote, local_pool_name,
					    remote_pool_name, image_name);
  r = replayer->start();
  if (r < 0) {
    std::cerr << "failed to start: " << cpp_strerror(r) << std::endl;
    goto cleanup;
  }

  while (!g_stopping.read()) {
    usleep(200000);
  }

  replayer->stop();

 cleanup:
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();

  delete replayer;
  g_ceph_context->put();

  return r < 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
