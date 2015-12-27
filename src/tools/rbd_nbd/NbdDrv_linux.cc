// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sys/types.h>

#include <arpa/inet.h>
#include <linux/nbd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

#include "include/buffer.h"
#include "include/byteorder.h"
#include "include/stringify.h"

#include "common/errno.h"
#include "common/module.h"
#include "common/safe_io.h"

#include "NbdDrv.h"

#ifdef CEPH_BIG_ENDIAN
#define ntohll(a) (a)
#elif defined(CEPH_LITTLE_ENDIAN)
#define ntohll(a) swab64(a)
#else
#error "Could not determine endianess"
#endif
#define htonll(a) ntohll(a)

namespace rbd {

class NbdReqImpl : public NbdReq {
public:
  NbdReqImpl() : NbdReq(), command(Unknown), length(0), offset(0) {
    memset(&reply, 0, sizeof(reply));
  }

  Command get_cmd() {
    return command;
  }

  size_t get_length() {
    return length;
  }

  uint64_t get_offset() {
    return offset;
  }

  bufferlist &get_data_ref() {
    return data;
  }

  void set_error(int error) {
    reply.error = htonl(error);
  }

private:
  friend class NbdDrvImpl;
  Command command;
  size_t length;
  uint64_t offset;
  bufferlist data;
  nbd_reply reply;
};

class NbdDrvImpl : public NbdDrv {
public:
  NbdDrvImpl(const std::string &devspec, size_t sectorsize, size_t mediasize,
             bool readonly) :  devpath(devspec), sectorsize(sectorsize),
			       mediasize(mediasize), readonly(readonly),
			       nbd(0), fd(0), run(false) {
  }

  ~NbdDrvImpl() {
  }

  int init() {
    int fd_[2];
    int r = 0;
    unsigned long flags;
    int read_only;

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd_) < 0) {
      return -errno;
    }

    if (devpath.empty()) {
      int index = 0;
      while (true) {
	std::string dev = "/dev/nbd" + stringify(index);

	nbd = open(dev.c_str(), O_RDWR);
	if (nbd < 0) {
	  r = nbd;
	  std::cerr << "rbd-nbd: failed to find unused device: "
		    << cpp_strerror(r) << std::endl;
	  goto close_fd;
	}

	if (ioctl(nbd, NBD_SET_SOCK, fd_[0]) < 0) {
	  close(nbd);
	  ++index;
	  continue;
	}

	devpath = dev;
	break;
      }
    } else {
      nbd = open(devpath.c_str(), O_RDWR);
      if (nbd < 0) {
	r = nbd;
	std::cerr << "rbd-nbd: failed to open device " << devpath << ": "
		  << cpp_strerror(r) << std::endl;
	goto close_fd;
      }

      if (ioctl(nbd, NBD_SET_SOCK, fd_[0]) < 0) {
	r = -errno;
	std::cerr << "rbd-nbd: device " << devpath << " is busy" << std::endl;
	close(nbd);
	goto close_fd;
      }
    }

    flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_HAS_FLAGS;
    if (readonly)
      flags |= NBD_FLAG_READ_ONLY;

    if (ioctl(nbd, NBD_SET_BLKSIZE, (unsigned long)sectorsize) < 0) {
      r = -errno;
      std::cerr << "ioctl(NBD_SET_BLKSIZE, " << sectorsize << ") failed: "
		<< cpp_strerror(r) << std::endl;
      goto close_nbd;
    }

    if (ioctl(nbd, NBD_SET_SIZE, (unsigned long)mediasize) < 0) {
      r = -errno;
      std::cerr << "ioctl(NBD_SET_SIZE, " << mediasize << ") failed: "
		<< cpp_strerror(r) << std::endl;
      goto close_nbd;
    }

    ioctl(nbd, NBD_SET_FLAGS, flags);

    read_only = readonly ? 1 : 0;
    if (ioctl(nbd, BLKROSET, (unsigned long)&read_only) < 0) {
      r = -errno;
      std::cerr << "ioctl(BLKROSET, " << read_only << ") failed: "
		<< cpp_strerror(r) << std::endl;
      goto close_nbd;
    }

    fd = fd_[1];
    run = true;
    return 0;

  close_nbd:
    if (r < 0) {
      ioctl(nbd, NBD_CLEAR_SOCK);
    }
    close(nbd);
  close_fd:
    close(fd_[0]);
    close(fd_[1]);
    return r;
  }

  void loop() {
    ioctl(nbd, NBD_DO_IT);
  }

  void shutdown() {
    ::shutdown(fd, SHUT_RDWR);
    run = false;
  }

  void fini() {
    close(nbd);
    close(fd);
  }

  bool is_shutdown() {
    return !run;
  }

  void notify(size_t newsize) {
    if (ioctl(fd, BLKFLSBUF, NULL) < 0) {
      std::cerr << "invalidate page cache failed: " << cpp_strerror(errno)
		<< std::endl;
    }
    if (ioctl(fd, NBD_SET_SIZE, (unsigned long)newsize) < 0) {
      std::cerr << "resize failed: " << cpp_strerror(errno) << std::endl;
    }
    mediasize = newsize;
  }

  int recv(NbdReq **req_) {
    static size_t count;
    nbd_request request;

    assert(sizeof(request) == sizeof(struct nbd_request));

    int r = safe_read_exact(fd, &request, sizeof(request));
    if (r < 0) {
      std::cerr << "read request failed: " << cpp_strerror(r) << std::endl;
      return r;
    }

    if (request.magic != htonl(NBD_REQUEST_MAGIC)) {
      std::cerr << "invalid request" << std::endl;
      return -EINVAL;
    }

    int cmd = ntohl(request.type) & 0x0000ffff;

    if (cmd == NBD_CMD_DISC) {
      shutdown();
      return -EINTR;
    }

    NbdReqImpl *req = new NbdReqImpl();

    switch (cmd) {
    case NBD_CMD_WRITE:
      req->command = NbdReq::Write;
      break;
    case NBD_CMD_READ:
      req->command = NbdReq::Read;
      break;
    case NBD_CMD_FLUSH:
      req->command = NbdReq::Flush;
      break;
    case NBD_CMD_TRIM:
      req->command = NbdReq::Discard;
      break;
    default:
      req->command = NbdReq::Unknown;
    }

    req->offset = ntohll(request.from);
    req->length = ntohl(request.len);
    req->reply.magic = htonl(NBD_REPLY_MAGIC);
    memcpy(req->reply.handle, request.handle, sizeof(req->reply.handle));

    if (req->command == NbdReq::Write) {
      bufferptr ptr(req->length);
      r = safe_read_exact(fd, ptr.c_str(), req->length);
      if (r < 0) {
	std::cerr << "read request data failed: " << cpp_strerror(r)
		  << std::endl;
	delete req;
	return r;
      }
      req->data.push_back(ptr);
    }

    *req_ = req;
    return 0;
  }

  int send(NbdReq *req_) {
    static size_t count;
    NbdReqImpl *req = static_cast<NbdReqImpl *>(req_);

    int r = safe_write(fd, &req->reply, sizeof(req->reply));
    if (r < 0) {
      std::cerr << "write reply failed: " << cpp_strerror(r) << std::endl;
      goto free;
    }

    if (req->command == NbdReq::Read && req->reply.error == htonl(0)) {
      assert(req->data.length() == req->length);
      r = req->data.write_fd(fd);
      if (r < 0) {
	std::cerr << "write reply data failed: " << cpp_strerror(r)
		  << std::endl;
	goto free;
      }
    }

  free:
    delete req_;
    return r;
  }

private:
  std::string devpath;
  size_t sectorsize;
  size_t mediasize;
  bool readonly;
  int nbd;
  int fd;
  bool run;
};

int NbdDrv::load(int nbds_max) {
  const char *param = NULL;

  if (access("/sys/module/nbd", F_OK) == 0) {
    return 0;
  }

  if (nbds_max > 0) {
    std::ostringstream param_;
    param_ << "nbds_max=" << nbds_max;
    param = param_.str().c_str();
  }

  return module_load("nbd", param);
}

int NbdDrv::create(const std::string &devspec, size_t sectorsize,
		   size_t mediasize, bool readonly, NbdDrv **drv_) {
  NbdDrvImpl *drv = new NbdDrvImpl(devspec, sectorsize, mediasize, readonly);
  int r = drv->init();
  if (r < 0) {
    delete drv;
    return r;
  }
  *drv_ = drv;
  return 0;
}

int NbdDrv::kill(const std::string &devpath) {

  int nbd = open(devpath.c_str(), O_RDWR);
  if (nbd < 0) {
    std::cerr << "failed to open device " << devpath << ": "
	      << cpp_strerror(nbd) << std::endl;
    return nbd;
  }

  if (ioctl(nbd, NBD_DISCONNECT) < 0) {
    std::cerr << "device is not used" << std::endl;
  }
  ioctl(nbd, NBD_CLEAR_SOCK);
  close(nbd);

  return 0;
}

static int nbd_dev_status(const char *path, bool *alive) {
  int fd[2], nbd, r;

  *alive = false;

  nbd = open(path, O_RDWR);
  if (nbd < 0) {
    return nbd;
  }

  r = 0;
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) < 0) {
    std::cerr << "failed to create socketpair: " << cpp_strerror(nbd)
	      << std::endl;
    r = -errno;
  } else {
    if (ioctl(nbd, NBD_SET_SOCK, fd[0]) == 0) {
      ioctl(nbd, NBD_CLEAR_SOCK);
    } else {
      *alive = true;
    }
    close(fd[0]);
    close(fd[1]);
  }

  close(nbd);
  return r;
}

void NbdDrv::list(std::list<std::string> &devs) {
  int m = 0;

  while (true) {
    std::string path = "/dev/nbd" + stringify(m);
    bool alive;
    if (nbd_dev_status(path.c_str(), &alive) < 0)
      break;
    if (alive)
      devs.push_back(path);
    m++;
  }
}

} // namespace rbd
