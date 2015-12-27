// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RBD_NBD_DRV_H
#define CEPH_RBD_NBD_DRV_H

namespace rbd {

class NbdReq {
public:
  enum Command {
    Unknown = 0,
    Write = 1,
    Read = 2,
    Flush = 3,
    Discard = 4,
  };

public:
  NbdReq() {}
  virtual ~NbdReq() {}

  virtual Command get_cmd() = 0;
  virtual bufferlist &get_data_ref() = 0;
  virtual size_t get_length() = 0;
  virtual uint64_t get_offset() = 0;

  virtual void set_error(int error) = 0;
};

class NbdDrv {
public:
  static int load(int nbds_max=0);
  static int create(const std::string &devspec, size_t sectorsize,
		    size_t mediasize, bool readonly, NbdDrv **drv);
  static int kill(const std::string &devspec);
  static void list(std::list<std::string> &devs);

public:
  NbdDrv() {}
  virtual ~NbdDrv() {}

  virtual int init() = 0;
  virtual void loop() = 0;
  virtual void shutdown() = 0;
  virtual void fini() = 0;

  virtual bool is_shutdown() = 0;

  virtual void notify(size_t newsize) = 0;

  virtual int recv(NbdReq **req) = 0;
  virtual int send(NbdReq *req) = 0;
};

} // namespace rbd

#endif //  CEPH_RBD_NBD_DRV_H
