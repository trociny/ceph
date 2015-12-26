// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"

#include <sys/types.h>

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include <iostream>
#include <boost/regex.hpp>

#include "mon/MonClient.h"
#include "common/config.h"

#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/Preforker.h"
#include "global/global_init.h"

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#include "NbdDrv.h"

static void usage()
{
  std::cout << "Usage: rbd-nbd [options] map <image-or-snap-spec>  Map a image to nbd device\n"
            << "               unmap <device path>                 Unmap nbd device\n"
            << "               list-mapped                         List mapped nbd devices\n"
            << "Options: --device <device path>                    Specify nbd device path\n"
            << "         --read-only                               Map readonly\n"
            << "         --nbds_max <limit>                        Override for module param\n"
            << std::endl;
}

static Preforker forker;
static std::string devpath, poolname("rbd"), imgname, snapname;
static bool readonly = false;
static int nbds_max = 0;

class NBDServer
{
private:
  rbd::NbdDrv *drv;
  librbd::Image &image;

public:
  NBDServer(rbd::NbdDrv *_drv, librbd::Image& _image)
    : drv(_drv)
    , image(_image)
    , terminated(false)
    , lock("NBDServer::Locker")
    , reader_thread(*this, &NBDServer::reader_entry)
    , writer_thread(*this, &NBDServer::writer_entry)
    , started(false)
  {}

private:
  atomic_t terminated;

  void shutdown()
  {
    if (terminated.compare_and_swap(false, true)) {
      drv->shutdown();

      Mutex::Locker l(lock);
      cond.Signal();
    }
  }

  struct IOContext
  {
    xlist<IOContext*>::item item;
    NBDServer *server;
    rbd::NbdReq *req;

    IOContext()
      : item(this)
    {}
  };

  Mutex lock;
  Cond cond;
  xlist<IOContext*> io_pending;
  xlist<IOContext*> io_finished;

  void io_start(IOContext *ctx)
  {
    Mutex::Locker l(lock);
    io_pending.push_back(&ctx->item);
  }

  void io_finish(IOContext *ctx)
  {
    Mutex::Locker l(lock);
    assert(ctx->item.is_on_list());
    ctx->item.remove_myself();
    io_finished.push_back(&ctx->item);
    cond.Signal();
  }

  IOContext *wait_io_finish()
  {
    Mutex::Locker l(lock);
    while(io_finished.empty() && !terminated.read())
      cond.Wait(lock);

    if (io_finished.empty())
      return NULL;

    IOContext *ret = io_finished.front();
    io_finished.pop_front();

    return ret;
  }

  void wait_clean()
  {
    assert(!reader_thread.is_started());
    Mutex::Locker l(lock);
    while(!io_pending.empty())
      cond.Wait(lock);

    while(!io_finished.empty()) {
      ceph::unique_ptr<IOContext> free_ctx(io_finished.front());
      io_finished.pop_front();
    }
  }

  static void aio_callback(librbd::completion_t cb, void *arg)
  {
    librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(cb);

    IOContext *ctx = reinterpret_cast<IOContext *>(arg);
    int ret = aio_completion->get_return_value();
    if (ret < 0) {
      ctx->req->set_error(-ret);
    } else {
      assert(ret == static_cast<int>(ctx->req->get_length()));
    }
    ctx->server->io_finish(ctx);

    aio_completion->release();
  }

  void reader_entry()
  {
    while (!terminated.read()) {
      ceph::unique_ptr<IOContext> ctx(new IOContext());
      ctx->server = this;
      int r = drv->recv(&ctx->req);
      if (r < 0) {
	if (r != -EINTR || !drv->is_shutdown()) {
          std::cerr << "rbd-nbd: nbd_drv_recv: " << cpp_strerror(r)
		    << std::endl;
	}
	return;
      }

      IOContext *pctx = ctx.release();
      io_start(pctx);
      librbd::RBD::AioCompletion *c = new librbd::RBD::AioCompletion(pctx, aio_callback);
      switch (pctx->req->get_cmd())
      {
      case rbd::NbdReq::Write:
	image.aio_write(pctx->req->get_offset(), pctx->req->get_length(),
			pctx->req->get_data(), c);
	break;
      case rbd::NbdReq::Read:
	image.aio_read(pctx->req->get_offset(), pctx->req->get_length(),
		       pctx->req->get_data(), c);
	break;
      case rbd::NbdReq::Flush:
	image.aio_flush(c);
	break;
      case rbd::NbdReq::Discard:
	image.aio_discard(pctx->req->get_offset(), pctx->req->get_length(), c);
	break;
      default:
	return;
      }
    }
  }

  void writer_entry()
  {
    while (!terminated.read()) {
      ceph::unique_ptr<IOContext> ctx(wait_io_finish());
      if (!ctx)
        return;

      int r = drv->send(ctx->req);
      if (r < 0) {
	std::cerr << "rbd-nbd: nbd_drv_send: " << cpp_strerror(r)
		  << std::endl;
	return;
      }
    }
  }

  class ThreadHelper : public Thread
  {
  public:
    typedef void (NBDServer::*entry_func)();
  private:
    NBDServer &server;
    entry_func func;
  public:
    ThreadHelper(NBDServer &_server, entry_func _func)
      :server(_server)
      ,func(_func)
    {}
  protected:
    virtual void* entry()
    {
      (server.*func)();
      server.shutdown();
      return NULL;
    }
  } reader_thread, writer_thread;

  bool started;
public:
  void start()
  {
    if (!started) {
      started = true;

      reader_thread.create();
      writer_thread.create();
    }
  }

  void stop()
  {
    if (started) {
      shutdown();

      reader_thread.join();
      writer_thread.join();

      wait_clean();

      started = false;
    }
  }

  ~NBDServer()
  {
    stop();
  }
};


class NBDWatchCtx : public librados::WatchCtx2
{
private:
  rbd::NbdDrv *drv;
  librados::IoCtx &io_ctx;
  librbd::Image &image;
  std::string header_oid;
  uint64_t size;
public:
  NBDWatchCtx(rbd::NbdDrv *_drv,
              librados::IoCtx &_io_ctx,
              librbd::Image &_image,
              std::string &_header_oid,
              unsigned long _size)
    : drv(_drv)
    , io_ctx(_io_ctx)
    , image(_image)
    , header_oid(_header_oid)
    , size(_size)
  { }

  virtual ~NBDWatchCtx() {}

  virtual void handle_notify(uint64_t notify_id,
                             uint64_t cookie,
                             uint64_t notifier_id,
                             bufferlist& bl)
  {
    librbd::image_info_t info;
    if (image.stat(info, sizeof(info)) == 0) {
      uint64_t new_size = info.size;

      if (new_size != size) {
	drv->notify(new_size);
        if (image.invalidate_cache() < 0)
          std::cerr << "rbd-nbd: invalidate rbd cache failed" << std::endl;
        size = new_size;
      }
    }

    bufferlist reply;
    io_ctx.notify_ack(header_oid, notify_id, cookie, reply);
  }

  virtual void handle_error(uint64_t cookie, int err)
  {
    //ignore
  }
};

static int do_map()
{
  int r;

  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;

  rbd::NbdDrv *drv;

  int null_fd = -1;

  uint8_t old_format;
  librbd::image_info_t info;

  rbd::NbdDrv::load(nbds_max);

  r = rados.init_with_context(g_ceph_context);
  if (r < 0)
    goto close_ret;

  r = rados.connect();
  if (r < 0)
    goto close_ret;

  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0)
    goto close_ret;

  r = rbd.open(io_ctx, image, imgname.c_str());
  if (r < 0)
    goto close_ret;

  if (!snapname.empty()) {
    r = image.snap_set(snapname.c_str());
    if (r < 0)
      goto close_ret;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0)
    goto close_ret;

  r = image.old_format(&old_format);
  if (r < 0)
    goto close_ret;

  r = rbd::NbdDrv::create(devpath.c_str(), 512, info.size,
		     readonly || !snapname.empty(), &drv);
  if (r < 0) {
    r = -errno;
    goto close_ret;
  }

  {
    string header_oid;
    uint64_t watcher;

    if (old_format != 0) {
      header_oid = imgname + RBD_SUFFIX;
    } else {
      char prefix[RBD_MAX_BLOCK_NAME_SIZE + 1];
      strncpy(prefix, info.block_name_prefix, RBD_MAX_BLOCK_NAME_SIZE);
      prefix[RBD_MAX_BLOCK_NAME_SIZE] = '\0';

      std::string image_id(prefix + strlen(RBD_DATA_PREFIX));
      header_oid = RBD_HEADER_PREFIX + image_id;
    }

    NBDWatchCtx watch_ctx(drv, io_ctx, image, header_oid, info.size);
    r = io_ctx.watch2(header_oid, &watcher, &watch_ctx);
    if (r < 0)
      goto close_nbd;

    if (g_conf->daemonize) {
      r = open("/dev/null", O_RDWR);
      if (r < 0)
        goto close_watcher;
      null_fd = r;
    }

    cout << devpath << std::endl;

    if (g_conf->daemonize) {
      forker.daemonize();

      ::dup2(null_fd, STDIN_FILENO);
      ::dup2(null_fd, STDOUT_FILENO);
      ::dup2(null_fd, STDERR_FILENO);
      close(null_fd);
    }

    {
      NBDServer server(drv, image);

      server.start();
      drv->loop();
      server.stop();
    }

close_watcher:
    io_ctx.unwatch2(watcher);
  }

close_nbd:
  drv->fini();
  delete drv;
close_ret:
  image.close();
  io_ctx.close();
  rados.shutdown();
  return r;
}

static int do_unmap()
{
  return rbd::NbdDrv::kill(devpath.c_str());
}

static int parse_imgpath(const std::string &imgpath)
{
  boost::regex pattern("^(?:([^/@]+)/)?([^/@]+)(?:@([^/@]+))?$");
  boost::smatch match;
  if (!boost::regex_match(imgpath, match, pattern)) {
    std::cerr << "rbd-nbd: invalid spec '" << imgpath << "'" << std::endl;
    return -EINVAL;
  }

  if (match[1].matched)
    poolname = match[1];

  imgname = match[2];

  if (match[3].matched)
    snapname = match[3];

  return 0;
}

static void list_mapped_devices()
{
  std::list<std::string> devs;

  rbd::NbdDrv::list(devs);

  for (std::list<std::string>::const_iterator i = devs.begin(); i != devs.end();
       i++) {
    cout << *i << std::endl;
  }
}

static int rbd_nbd(int argc, const char *argv[])
{
  int r;
  enum {
    None,
    Connect,
    Disconnect,
    List
  } cmd = None;

  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
              CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  std::vector<const char*>::iterator i;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &devpath, "--device", (char *)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &nbds_max, cerr, "--nbds_max", (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      readonly = true;
    } else {
      ++i;
    }
  }

  if (args.begin() != args.end()) {
    if (strcmp(*args.begin(), "map") == 0) {
      cmd = Connect;
    } else if (strcmp(*args.begin(), "unmap") == 0) {
      cmd = Disconnect;
    } else if (strcmp(*args.begin(), "list-mapped") == 0) {
      cmd = List;
    } else {
      cerr << "rbd-nbd: unknown command: " << *args.begin() << std::endl;
      return EXIT_FAILURE;
    }
    args.erase(args.begin());
  }

  if (cmd == None) {
    cerr << "rbd-nbd: must specify command" << std::endl;
    return EXIT_FAILURE;
  }

  switch (cmd) {
    case Connect:
      if (args.begin() == args.end()) {
        cerr << "rbd-nbd: must specify image-or-snap-spec" << std::endl;
        return EXIT_FAILURE;
      }
      if (parse_imgpath(string(*args.begin())) < 0)
        return EXIT_FAILURE;
      args.erase(args.begin());
      break;
    case Disconnect:
      if (args.begin() == args.end()) {
        cerr << "rbd-nbd: must specify nbd device path" << std::endl;
        return EXIT_FAILURE;
      }
      devpath = *args.begin();
      args.erase(args.begin());
      break;
    default:
      //shut up gcc;
      break;
  }

  if (args.begin() != args.end()) {
    cerr << "rbd-nbd: unknown args: " << *args.begin() << std::endl;
    return EXIT_FAILURE;
  }

  switch (cmd) {
    case Connect:
      common_init_finish(g_ceph_context);

      if (imgname.empty()) {
        cerr << "rbd-nbd: image name was not specified" << std::endl;
        return EXIT_FAILURE;
      }

      r = do_map();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    case Disconnect:
      r = do_unmap();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    case List:
      list_mapped_devices();
      break;
    default:
      usage();
      return EXIT_FAILURE;
  }

  return 0;
}

int main(int argc, const char *argv[])
{
  std::string err;

  if (forker.prefork(err) < 0) {
    cerr << err << std::endl;
    return EXIT_FAILURE;
  }

  if (forker.is_child()) {
    forker.exit(rbd_nbd(argc, argv));
  } else if (forker.parent_wait(err) < 0) {
    cerr << err << std::endl;
    return EXIT_FAILURE;
  } else {
    return 0;
  }
}
