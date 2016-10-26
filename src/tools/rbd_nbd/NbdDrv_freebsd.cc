// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sys/types.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "common/errno.h"
#include "common/safe_io.h"

#include "nbd_drv_freebsd.h"

#include "NbdDrv.h"

namespace rbd {

class NbdReqImpl : public NbdReq {
public:
  NbdReqImpl(nbd_drv_req_t req = 0) : NbdReq(), req(req) {
  }

  Command get_cmd() {
    return static_cast<Command>(nbd_drv_req_cmd(req));
  }

  size_t get_length() {
    return nbd_drv_req_length(req);
  }

  uint64_t get_offset() {
    return nbd_drv_req_offset(req);
  }

  bufferlist &get_data_ref() {
    return data;
  }

  void set_error(int error) {
    nbd_drv_req_set_error(req, error);
  }

private:
  friend class NbdDrvImpl;
  nbd_drv_req_t req;
  bufferlist data;
};

class NbdDrvImpl : public NbdDrv {
public:
  NbdDrvImpl(const std::string &devspec, size_t sectorsize, size_t mediasize,
             bool readonly) :  devspec(devspec), sectorsize(sectorsize),
			       mediasize(mediasize), readonly(readonly),
			       drv(0) {
  }

  ~NbdDrvImpl() {
  }

  int init() {

    int r = nbd_drv_create(devspec.c_str(), sectorsize, mediasize, readonly,
			   &drv);
    if (r < 0) {
      return r;
    }
    return 0;
  }

  void loop() {

    nbd_drv_loop(drv);
  }

  void shutdown() {

    nbd_drv_shutdown(drv);
  }

  void fini() {

    nbd_drv_destroy(drv);
  }

  void notify(size_t newsize) {

    nbd_drv_destroy(drv);
    mediasize = newsize;
  }

  int recv(NbdReq **req_) {
    nbd_drv_req_t req;

    int r = nbd_drv_recv(drv, &req);
    if (r < 0) {
      return r;
    }

    NbdReqImpl *reqimpl = new NbdReqImpl(req);

    if (nbd_drv_req_cmd(req) == NBD_DRV_CMD_WRITE) {
      bufferptr ptr(buffer::create_static(nbd_drv_req_length(req),
				static_cast<char *>(nbd_drv_req_data(req))));
      reqimpl->data.push_back(ptr);
    }

    *req_ = reqimpl;
    return 0;
  }

  int send(NbdReq *req_) {
    NbdReqImpl *req = static_cast<NbdReqImpl *>(req_);

    if (nbd_drv_req_cmd(req->req) == NBD_DRV_CMD_READ &&
	nbd_drv_req_error(req->req) == 0) {
      assert(req->data.length() == nbd_drv_req_length(req->req));
      nbd_drv_req_set_data(req->req, req->data.c_str());
    }

    int r = nbd_drv_send(drv, req->req);

    delete req_;
    return r;
  }

private:
  std::string devspec;
  size_t sectorsize;
  size_t mediasize;
  bool readonly;
  nbd_drv_t drv;
};

int NbdDrv::load(int nbds_max) {

  return nbd_drv_load(NULL);
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

  return 0;
}

void NbdDrv::list(std::list<std::string> &devs) {
  int m = 0;

  while (true) {
    std::string path = "/dev/XXXXMG"; //+ stringify(m);
    bool alive;
    if (nbd_drv_status(path.c_str(), &alive) < 0)
      break;
    if (alive)
      devs.push_back(path);
    m++;
  }
}

} // namespace rbd
