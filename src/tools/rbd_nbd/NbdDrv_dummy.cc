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

#include <errno.h>

#include "NbdDrv.h"

namespace rbd {

int NbdDrv::create(const std::string &devspec, size_t sectorsize,
		   size_t mediasize, bool readonly, NbdDrv **drv_) {
  return -ENOTSUP;
}

int NbdDrv::kill(const std::string &devpath) {
  return -ENOTSUP;
}

int NbdDrv::list(std::list<std::string> &devs) {
  return -ENOTSUP;
}

int NbdDrv::load(int nbds_max) {
  return -ENOTSUP;
}

} // namespace rbd
