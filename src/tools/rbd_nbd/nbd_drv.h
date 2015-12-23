// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_NBD_DRV_H
#define CEPH_RBD_NBD_DRV_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

typedef void *nbd_drv_t;
typedef void *nbd_drv_req_t;

/* NBD driver commands */
enum {
  NBD_DRV_CMD_UNKNOWN = 0,
  NBD_DRV_CMD_WRITE = 1,
  NBD_DRV_CMD_READ = 2,
  NBD_DRV_CMD_FLUSH = 3,
  NBD_DRV_CMD_DISCARD = 4,
};

int nbd_drv_load(const char *param);

int nbd_drv_create(const char *devspec, size_t sectorsize, size_t mediasize,
  bool readonly, nbd_drv_t *drv);
void nbd_drv_loop(nbd_drv_t drv);
void nbd_drv_shutdown(nbd_drv_t drv);
void nbd_drv_destroy(nbd_drv_t drv);

bool nbd_drv_is_shutdown(nbd_drv_t drv_);

int nbd_drv_recv(nbd_drv_t drv, nbd_drv_req_t *req);
int nbd_drv_send(nbd_drv_t drv, nbd_drv_req_t req);

void nbd_drv_notify(nbd_drv_t drv, size_t newsize);

int nbd_drv_req_cmd(nbd_drv_req_t req);
void *nbd_drv_req_data(nbd_drv_req_t req);
size_t nbd_drv_req_length(nbd_drv_req_t req);
uint64_t nbd_drv_req_offset(nbd_drv_req_t req);
void nbd_drv_req_set_error(nbd_drv_req_t req, int error);
void nbd_drv_req_set_data(nbd_drv_req_t req, void *data);

int nbd_drv_kill(const char *devspec);
int nbd_drv_status(char *devspec, bool *alive);

#ifdef __cplusplus
}
#endif

#endif //  CEPH_RBD_NBD_DRV_H
