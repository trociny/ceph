// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Snapshot.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include <boost/variant.hpp>
#include "include/Context.h"
#include "common/Cond.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Snapshot: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

class GetGroupVisitor : public boost::static_visitor<int> {
public:
  CephContext* cct;
  librados::IoCtx *image_ioctx;
  snap_group_namespace_t *group_snap;

  explicit GetGroupVisitor(CephContext* cct, librados::IoCtx *_image_ioctx,
                           snap_group_namespace_t *group_snap)
    : cct(cct), image_ioctx(_image_ioctx), group_snap(group_snap) {};

  template <typename T>
  inline int operator()(const T&) const {
    // ignore other than GroupSnapshotNamespace types.
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::GroupSnapshotNamespace& snap_namespace) {
    IoCtx group_ioctx;
    int r = util::create_ioctx(*image_ioctx, "group", snap_namespace.group_pool,
                               {}, &group_ioctx);
    if (r < 0) {
      return r;
    }

    cls::rbd::GroupSnapshot group_snapshot;

    std::string group_name;
    r = cls_client::dir_get_name(&group_ioctx, RBD_GROUP_DIRECTORY,
				 snap_namespace.group_id, &group_name);
    if (r < 0) {
      lderr(cct) << "failed to retrieve group name: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    string group_header_oid = util::group_header_name(snap_namespace.group_id);
    r = cls_client::group_snap_get_by_id(&group_ioctx,
					 group_header_oid,
					 snap_namespace.group_snapshot_id,
					 &group_snapshot);
    if (r < 0) {
      lderr(cct) << "failed to retrieve group snapshot: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    group_snap->group_pool = group_ioctx.get_id();
    group_snap->group_name = group_name;
    group_snap->group_snap_name = group_snapshot.name;
    return 0;
  }
};

class GetTrashVisitor : public boost::static_visitor<int> {
public:
  std::string* original_name;

  explicit GetTrashVisitor(std::string* original_name)
    : original_name(original_name) {
  }

  template <typename T>
  inline int operator()(const T&) const {
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::TrashSnapshotNamespace& snap_namespace) {
    *original_name = snap_namespace.original_name;
    return 0;
  }
};

class GetMirrorPrimaryVisitor : public boost::static_visitor<int> {
public:
  snap_mirror_primary_namespace_t *mirror_snap;

  explicit GetMirrorPrimaryVisitor(snap_mirror_primary_namespace_t *mirror_snap)
    : mirror_snap(mirror_snap) {
  }

  template <typename T>
  inline int operator()(const T&) const {
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::MirrorPrimarySnapshotNamespace& snap_namespace) {
    mirror_snap->demoted = snap_namespace.demoted;
    mirror_snap->mirror_peers = snap_namespace.mirror_peers;
    return 0;
  }
};

class GetMirrorNonPrimaryVisitor : public boost::static_visitor<int> {
public:
  snap_mirror_non_primary_namespace_t *mirror_snap;

  explicit GetMirrorNonPrimaryVisitor(
      snap_mirror_non_primary_namespace_t *mirror_snap)
    : mirror_snap(mirror_snap) {
  }

  template <typename T>
  inline int operator()(const T&) const {
    return -EINVAL;
  }

  inline int operator()(
      const cls::rbd::MirrorNonPrimarySnapshotNamespace& snap_namespace) {
    mirror_snap->primary_mirror_uuid = snap_namespace.primary_mirror_uuid;
    mirror_snap->primary_snap_id = snap_namespace.primary_snap_id;
    mirror_snap->copied = snap_namespace.copied;
    mirror_snap->copy_progress = snap_namespace.copy_progress;
    return 0;
  }
};

} // anonymous namespace

template <typename I>
int Snapshot<I>::get_group_namespace(I *ictx, uint64_t snap_id,
                                     snap_group_namespace_t *group_snap) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  GetGroupVisitor ggv = GetGroupVisitor(ictx->cct, &ictx->md_ctx, group_snap);
  r = boost::apply_visitor(ggv, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_trash_namespace(I *ictx, uint64_t snap_id,
                                     std::string* original_name) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  auto visitor = GetTrashVisitor(original_name);
  r = boost::apply_visitor(visitor, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_mirror_primary_namespace(
    I *ictx, uint64_t snap_id, snap_mirror_primary_namespace_t *mirror_snap) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  auto gmv = GetMirrorPrimaryVisitor(mirror_snap);
  r = boost::apply_visitor(gmv, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_mirror_non_primary_namespace(
    I *ictx, uint64_t snap_id,
    snap_mirror_non_primary_namespace_t *mirror_snap) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock image_locker{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  auto gmv = GetMirrorNonPrimaryVisitor(mirror_snap);
  r = boost::apply_visitor(gmv, snap_info->snap_namespace);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Snapshot<I>::get_namespace_type(I *ictx, uint64_t snap_id,
                                snap_namespace_type_t *namespace_type) {
  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  std::shared_lock l{ictx->image_lock};
  auto snap_info = ictx->get_snap_info(snap_id);
  if (snap_info == nullptr) {
    return -ENOENT;
  }

  *namespace_type = static_cast<snap_namespace_type_t>(
    cls::rbd::get_snap_namespace_type(snap_info->snap_namespace));
  return 0;
}

template <typename I>
int Snapshot<I>::remove(I *ictx, uint64_t snap_id) {
  ldout(ictx->cct, 20) << "snap_remove " << ictx << " " << snap_id << dendl;

  int r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  cls::rbd::SnapshotNamespace snapshot_namespace;
  std::string snapshot_name;
  {
    std::shared_lock image_locker{ictx->image_lock};
    auto it = ictx->snap_info.find(snap_id);
    if (it == ictx->snap_info.end()) {
      return -ENOENT;
    }

    snapshot_namespace = it->second.snap_namespace;
    snapshot_name = it->second.name;
  }

  C_SaferCond ctx;
  ictx->operations->snap_remove(snapshot_namespace, snapshot_name, &ctx);
  r = ctx.wait();
  return r;
}

template <typename I>
int Snapshot<I>::get_name(I *ictx, uint64_t snap_id, std::string *snap_name)
  {
    ldout(ictx->cct, 20) << "snap_get_name " << ictx << " " << snap_id << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    std::shared_lock image_locker{ictx->image_lock};
    r = ictx->get_snap_name(snap_id, snap_name);

    return r;
  }

template <typename I>
int Snapshot<I>::get_id(I *ictx, const std::string& snap_name, uint64_t *snap_id)
  {
    ldout(ictx->cct, 20) << "snap_get_id " << ictx << " " << snap_name << dendl;

    int r = ictx->state->refresh_if_required();
    if (r < 0)
      return r;

    std::shared_lock image_locker{ictx->image_lock};
    *snap_id = ictx->get_snap_id(cls::rbd::UserSnapshotNamespace(), snap_name);
    if (*snap_id == CEPH_NOSNAP)
      return -ENOENT;

    return 0;
  }

} // namespace api
} // namespace librbd

template class librbd::api::Snapshot<librbd::ImageCtx>;
