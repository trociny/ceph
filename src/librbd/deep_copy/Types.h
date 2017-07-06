// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_TYPES_H
#define CEPH_LIBRBD_DEEP_COPY_TYPES_H

#include <boost/optional.hpp>

namespace librbd {
namespace deep_copy {

typedef boost::optional<uint64_t> ObjectNumber;

} // namespace image
} // namespace librbd

#endif // CEPH_LIBRBD_DEEP_COPY_TYPES_H
