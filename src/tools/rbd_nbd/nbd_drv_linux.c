// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sys/types.h>

#include <arpa/inet.h>
#include <linux/nbd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "include/byteorder.h"

#include "common/module.h"
#include "common/safe_io.h"

#include "nbd_drv.h"

#ifdef CEPH_BIG_ENDIAN
#define ntohll(a) (a)
#elif defined(CEPH_LITTLE_ENDIAN)
#define ntohll(a) swab64(a)
#else
#error "Could not determine endianess"
#endif
#define htonll(a) ntohll(a)

struct nbd_drv {
  int fd;
  int nbd;
  bool shutdown;
};

struct nbd_drv_req {
  struct nbd_request request;
  struct nbd_reply reply;
  void *data;
};

int nbd_drv_load(const char *param) {

  if (access("/sys/module/nbd", F_OK) == 0) {
    return 0;
  }

  return module_load("nbd", param);
}

int nbd_drv_create(const char *devpath, size_t sectorsize, size_t mediasize,
  bool readonly, nbd_drv_t *drv_) {
  struct nbd_drv *drv;

  int fd[2];
  int nbd;
  int r = 0;
  unsigned long flags;
  int read_only;

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) < 0) {
    return -errno;
  }

  if (devpath == NULL || *devpath == 0) {
    char dev[64];
    int index = 0;
    while (true) {
      snprintf(dev, sizeof(dev), "/dev/nbd%d", index);

      nbd = open(dev, O_RDWR);
      if (nbd < 0) {
        r = nbd;
        warn("failed to find unused device");
        goto close_fd;
      }

      if (ioctl(nbd, NBD_SET_SOCK, fd[0]) < 0) {
        close(nbd);
        ++index;
        continue;
      }

      devpath = dev;
      break;
    }
  } else {
    nbd = open(devpath, O_RDWR);
    if (nbd < 0) {
      r = nbd;
      warn("failed to open device: %s", devpath);
      goto close_fd;
    }

    if (ioctl(nbd, NBD_SET_SOCK, fd[0]) < 0) {
      r = -errno;
      warn("device %s is busy", devpath);
      close(nbd);
      goto close_fd;
    }
  }

  flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_HAS_FLAGS;
  if (readonly)
    flags |= NBD_FLAG_READ_ONLY;

  if (ioctl(nbd, NBD_SET_BLKSIZE, (unsigned long)sectorsize) < 0) {
    warn("ioctl(NBD_SET_BLKSIZE, %lu) failed", (unsigned long)sectorsize);
    r = -errno;
    goto close_nbd;
  }

  if (ioctl(nbd, NBD_SET_SIZE, (unsigned long)mediasize) < 0) {
    warn("ioctl(NBD_SET_SIZE, %lu) failed", (unsigned long)mediasize);
    r = -errno;
    goto close_nbd;
  }

  ioctl(nbd, NBD_SET_FLAGS, flags);

  read_only = readonly ? 1 : 0;
  r = ioctl(nbd, BLKROSET, (unsigned long)&read_only);
  if (r < 0) {
    warn("ioctl(BLKROSET, %d) failed", read_only);
    r = -errno;
    goto close_nbd;
  }

  drv = calloc(1, sizeof(*drv));
  if (drv == NULL) {
    r = -ENOMEM;
    goto close_nbd;
  }

  drv->fd = fd[1];
  drv->nbd = nbd;
  *drv_ = (nbd_drv_t)drv;
  return 0;

close_nbd:
  if (r < 0) {
    ioctl(nbd, NBD_CLEAR_SOCK);
  }
  close(nbd);
close_fd:
  close(fd[0]);
  close(fd[1]);
  return r;
}

void nbd_drv_loop(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  ioctl(drv->nbd, NBD_DO_IT);
}

void nbd_drv_shutdown(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  shutdown(drv->fd, SHUT_RDWR);
  drv->shutdown = true;
}

bool nbd_drv_is_shutdown(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  return drv->shutdown;
}

void nbd_drv_destroy(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  close(drv->nbd);
  close(drv->fd);
  free(drv);
}

int nbd_drv_recv(nbd_drv_t drv_, nbd_drv_req_t *req_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;
  struct nbd_drv_req *req;
  struct nbd_request request;
  void *data = NULL;
  int r;

  r = safe_read_exact(drv->fd, &request, sizeof(request));
  if (r < 0) {
    warn("read request failed");
    return r;
  }

  if (request.magic != htonl(NBD_REQUEST_MAGIC)) {
    warn("invalid request");
    return -EINVAL;
  }

  request.from = ntohll(request.from);
  request.type = ntohl(request.type);
  request.len = ntohl(request.len);

  int command = request.type & 0x0000ffff;

  switch (command)
  {
  case NBD_CMD_DISC:
    nbd_drv_shutdown(drv);
    return -EINTR;
  case NBD_CMD_WRITE:
    data = malloc(request.len);
    if (data == NULL) {
      return -ENOMEM;
    }
    r = safe_read_exact(drv->fd, data, request.len);
    if (r < 0)
      warn("read request data failed");
      free(data);
      return r;
    break;
  }

  req = calloc(1, sizeof(*req));
  if (drv == NULL) {
    free(data);
    return -ENOMEM;
  }

  req->request = request;
  req->reply.magic = htonl(NBD_REPLY_MAGIC);
  memcpy(req->reply.handle, req->request.handle, sizeof(req->reply.handle));
  *req_ = (nbd_drv_req_t)req;
  return 0;
}

int nbd_drv_send(nbd_drv_t drv_, nbd_drv_req_t req_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;
  int r = 0;

  r = safe_write(drv->fd, &req->reply, sizeof(req->reply));
  if (r < 0) {
    warn("write reply failed");
    goto free;
  }

  int command = req->request.type & 0x0000ffff;
  if (command == NBD_CMD_READ && req->reply.error == htonl(0)) {
    r = safe_write(drv->fd, req->data, req->request.len);
    if (r < 0)
      warn("write reply data failed");
  }

free:
  free(req);
  return r;
}

void nbd_drv_notify(nbd_drv_t drv_, size_t newsize) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  if (ioctl(drv->fd, BLKFLSBUF, NULL) < 0)
    warn("invalidate page cache failed");
  if (ioctl(drv->fd, NBD_SET_SIZE, (unsigned long)newsize) < 0)
    warn("resize failed");
}

int nbd_drv_req_cmd(nbd_drv_req_t req_) {
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;
  int command = req->request.type & 0x0000ffff;

  switch (command) {
  case NBD_CMD_WRITE:
    return NBD_DRV_CMD_WRITE;
  case NBD_CMD_READ:
    return NBD_DRV_CMD_READ;
  case NBD_CMD_FLUSH:
    return NBD_DRV_CMD_FLUSH;
  case NBD_CMD_TRIM:
    return NBD_DRV_CMD_DISCARD;
  default:
    return NBD_DRV_CMD_UNKNOWN;
  }
}

uint64_t nbd_drv_req_offset(nbd_drv_req_t req_) {
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;

  return req->request.from;
}

void *nbd_drv_req_data(nbd_drv_req_t req_) {
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;

  return req->data;
}

size_t nbd_drv_req_length(nbd_drv_req_t req_) {
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;

  return req->request.len;
}

void nbd_drv_req_set_error(nbd_drv_req_t req_, int error) {
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;

  req->reply.error = htonl(error);
}

void nbd_drv_req_set_data(nbd_drv_req_t req_, void *data) {
  struct nbd_drv_req *req = (struct nbd_drv_req *)req_;

  req->data = data;
}

int nbd_drv_kill(const char *devpath) {
  int nbd = open(devpath, O_RDWR);
  if (nbd < 0) {
    warn("failed to open device: %s", devpath);
    return nbd;
  }

  if (ioctl(nbd, NBD_DISCONNECT) < 0)
    warn("device is not used");
  ioctl(nbd, NBD_CLEAR_SOCK);
  close(nbd);

  return 0;
}

int nbd_drv_status(char *path, bool *alive) {
  int fd[2], nbd, r;

  *alive = false;

  nbd = open(path, O_RDWR);
  if (nbd < 0) {
    return nbd;
  }

  r = 0;
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) < 0) {
    warn("failed to create socketpair");
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
