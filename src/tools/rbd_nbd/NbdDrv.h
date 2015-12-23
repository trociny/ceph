// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * rbd-nbd - RBD in userspace
 *
 * Copyright (C) 2016 Mirantis Inc
 *
 * Author: Mykola Golub <mgolub@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_RBD_NBD_DRV_H
#define CEPH_RBD_NBD_DRV_H

#include <list>
#include <string>

#include "include/buffer.h"

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

  virtual std::string get_id() = 0;
  virtual Command get_cmd() = 0;
  virtual bufferlist &get_data_ref() = 0;
  virtual size_t get_length() = 0;
  virtual uint64_t get_offset() = 0;
  virtual uint64_t get_error() = 0;

  virtual void set_error(int error) = 0;
};

class NbdDrv {
public:
  static int load(int nbds_max=0);
  static int create(const std::string &devspec, size_t sectorsize,
		    size_t mediasize, bool readonly, NbdDrv **drv);
  static int kill(const std::string &devspec);
  static int list(std::list<std::string> &devs);

public:
  NbdDrv() {}
  virtual ~NbdDrv() {}

  virtual int init() = 0;
  virtual void loop() = 0;
  virtual void shutdown() = 0;
  virtual void fini() = 0;

  virtual std::string get_devpath() = 0;

  virtual void notify(size_t newsize) = 0;

  virtual int recv(NbdReq **req) = 0;
  virtual int send(NbdReq *req) = 0;
};

} // namespace rbd

#endif //  CEPH_RBD_NBD_DRV_H
