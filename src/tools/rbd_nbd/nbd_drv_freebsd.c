// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/param.h>
#include <sys/bio.h>
#include <sys/disk.h>
#include <sys/linker.h>
#include <sys/stat.h> // XXXMG

#include <geom/gate/g_gate.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "nbd_drv_freebsd.h"

struct nbd_drv {
  int ggatefd;
  int ggateunit;
  bool run;
};

int nbd_drv_load(const char *param) {
  if (modfind("g_gate") != -1) {
    /* Present in kernel. */
    return 0;
  }

  if (kldload("geom_gate") == -1 || modfind("g_gate") == -1) {
    if (errno != EEXIST) {
      warn("failed to load geom_gate module");
      return -errno;
    }
  }
  return 0;
}

int nbd_drv_create(const char *devpath, size_t sectorsize, size_t mediasize,
  bool readonly, nbd_drv_t *drv_) {
  struct nbd_drv *drv;
  struct g_gate_ctl_create ggiocreate;

  warnx("%s: enter",  __func__);

  /*
   * We communicate with ggate via /dev/ggctl. Open it.
   */
  int ggatefd = open("/dev/" G_GATE_CTL_NAME, O_RDWR);
  if (ggatefd == -1) {
    warn("failed to open /dev/" G_GATE_CTL_NAME);
    return -errno;
  }

  drv = calloc(1, sizeof(*drv));
  if (drv == NULL) {
    errno = -ENOMEM;
    goto fail_close;
  }

  /*
   * Create provider.
   */
  memset(&ggiocreate, 0, sizeof(ggiocreate));
  ggiocreate.gctl_version = G_GATE_VERSION;
  ggiocreate.gctl_mediasize = mediasize;
  ggiocreate.gctl_sectorsize = sectorsize;
  ggiocreate.gctl_flags = readonly ? G_GATE_FLAG_READONLY : 0;
  ggiocreate.gctl_maxcount = 0;
  ggiocreate.gctl_timeout = 0;
  ggiocreate.gctl_unit = G_GATE_UNIT_AUTO;
  snprintf(ggiocreate.gctl_info, sizeof(ggiocreate.gctl_info), "%s", "XXXMG");
  if (ioctl(ggatefd, G_GATE_CMD_CREATE, &ggiocreate) == -1) {
    warn("failed to create " G_GATE_PROVIDER_NAME " device");
    goto fail;
  }
  printf("/dev/%s%d", G_GATE_PROVIDER_NAME, ggiocreate.gctl_unit); // XXXMG

  drv->ggatefd = ggatefd;
  drv->ggateunit = ggiocreate.gctl_unit;
  drv->run = true;
  *drv_ = drv;
  warnx("%s: return 0",  __func__);
  return 0;

fail:
  free(drv);
fail_close:
  close(ggatefd);
  warnx("%s: return %d",  __func__, -errno);
  return -errno;
}

void nbd_drv_loop(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  warnx("%s: enter",  __func__);
  // XXXMG
  while (drv->run) {
    warnx("%s: sleep(1)",  __func__);
    sleep(1);
  }
}

void nbd_drv_shutdown(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

  drv->run = false;
}

void nbd_drv_destroy(nbd_drv_t drv_) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;
  struct g_gate_ctl_destroy ggiod;

  warnx("%s: enter",  __func__);
  memset(&ggiod, 0, sizeof(ggiod));
  ggiod.gctl_version = G_GATE_VERSION;
  ggiod.gctl_unit = drv->ggateunit;
  ggiod.gctl_force = 1;

  // Remember errno.
  int rerrno = errno;

  if (ioctl(drv->ggatefd, G_GATE_CMD_DESTROY, &ggiod) == -1) {
    warn("failed to destroy /dev/%s%u device", G_GATE_PROVIDER_NAME,
	 drv->ggateunit);
  }
  // Restore errno.
  errno = rerrno;

  warnx("%s: return",  __func__);
  free(drv);
}

int nbd_drv_recv(nbd_drv_t drv_, nbd_drv_req_t *req) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;
  struct g_gate_ctl_io *ggio;
  int error, r;

  warnx("%s: enter",  __func__);
  ggio = calloc(1, sizeof(*ggio));
  if (ggio == NULL) {
    return -ENOMEM;
  }

  ggio->gctl_version = G_GATE_VERSION;
  ggio->gctl_unit = drv->ggateunit;
  ggio->gctl_data = malloc(MAXPHYS);
  ggio->gctl_length = MAXPHYS;

  // dout(10) << __func__ << ": " << ctx
  // 	   << ": Waiting for request from kernel." << dendl;
  warnx("%s: Waiting for request from kernel",  __func__);
  if (ioctl(drv->ggatefd, G_GATE_CMD_START, ggio) == -1) {
    warn("%s: G_GATE_CMD_START failed", __func__);
    return -errno;
  }
  // dout(10) << __func__ << ": " << ctx
  // 	   << ": Got request from kernel." << dendl;
  warnx("%s: Got request from kernel: seq=%d, unit=%d, cmd=%d, length=%d, error=%d, data=%p",  __func__, (int)ggio->gctl_seq, (int)ggio->gctl_unit, (int)ggio->gctl_cmd, (int)ggio->gctl_length, (int)ggio->gctl_error, ggio->gctl_data);
  error = ggio->gctl_error;
  switch (error) {
  case 0:
    break;
  case ECANCELED:
    // Exit gracefully.
    drv->run = false;
    r = -error;
    goto fail;
  case ENOMEM:
    /*
     * Buffer too small? Impossible, we allocate MAXPHYS
     * bytes - request can't be bigger than that.
     */
    /* FALLTHROUGH */
  case ENXIO:
  default:
    errno = error;
    warn("%s: G_GATE_CMD_START failed", __func__);
    r = -error;
    goto fail;
  }

  *req = ggio;
  warnx("%s: return 0", __func__);
  return 0;

fail:
  free(ggio->gctl_data);
  free(ggio);
  warnx("%s: return %d", __func__, r);
  return -r;
}

int nbd_drv_send(nbd_drv_t drv_, nbd_drv_req_t req) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;
  int r;

  // dout(10) << __func__ << ": " << ctx << ": Got request." << dendl;

  r = 0;

  warnx("%s: Send request to kernel: seq=%d, unit=%d, cmd=%d, length=%d, error=%d, data=%p",  __func__, (int)ggio->gctl_seq, (int)ggio->gctl_unit, (int)ggio->gctl_cmd, (int)ggio->gctl_length, (int)ggio->gctl_error, ggio->gctl_data);
  if (ioctl(drv->ggatefd, G_GATE_CMD_DONE, ggio) == -1) {
    warn("%s: G_GATE_CMD_DONE failed", __func__);
    r = -errno;
  }

  free(ggio->gctl_data);
  free(ggio);
  warnx("%s: return %d", __func__, r);
  return r;
}

void nbd_drv_notify(nbd_drv_t drv_, size_t newsize) {
  struct nbd_drv *drv = (struct nbd_drv *)drv_;

}

int nbd_drv_req_cmd(nbd_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  switch (ggio->gctl_cmd) {
  case BIO_WRITE:
    return NBD_DRV_CMD_WRITE;
  case BIO_READ:
    return NBD_DRV_CMD_READ;
  case BIO_FLUSH:
    return NBD_DRV_CMD_FLUSH;
  case BIO_DELETE:
    return NBD_DRV_CMD_DISCARD;
  default:
    return NBD_DRV_CMD_UNKNOWN;
  }
}

uint64_t nbd_drv_req_offset(nbd_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_offset;
}

size_t nbd_drv_req_length(nbd_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_length;
}

void *nbd_drv_req_data(nbd_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_data;
}

int nbd_drv_req_error(nbd_drv_req_t req) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  return ggio->gctl_error;
}

void nbd_drv_req_set_data(nbd_drv_req_t req, void *data) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  warnx("%s: seq=%d, unit=%d, cmd=%d, length=%d, error=%d, data=%p",  __func__, (int)ggio->gctl_seq, (int)ggio->gctl_unit, (int)ggio->gctl_cmd, (int)ggio->gctl_length, (int)ggio->gctl_error, ggio->gctl_data);
  //warnx("%s: free(%p)",  __func__,  ggio->gctl_data);
  //free(ggio->gctl_data);
  //warnx("%s: data = %p",  __func__, data);
  memcpy(ggio->gctl_data, data, ggio->gctl_length); // XXXMG
}

void nbd_drv_req_set_error(nbd_drv_req_t req, int error) {
  struct g_gate_ctl_io *ggio = (struct g_gate_ctl_io *)req;

  ggio->gctl_error = error;
}

int nbd_drv_kill(const char *devpath) {

  // ggioc.gctl_version = G_GATE_VERSION;
  // ggioc.gctl_unit = unit;
  // ggioc.gctl_seq = 0;
  // g_gate_ioctl(G_GATE_CMD_CANCEL, &ggioc);
  return 0;
}

int nbd_drv_status(const char *path, bool *alive) {

  return -ENOTSUP;
}
