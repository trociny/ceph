// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror_snapshot/CreatePrimaryRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror_snapshot/UnlinkPeerRequest.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror_snapshot::CreatePrimaryRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror_snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void CreatePrimaryRequest<I>::send() {
  refresh_image();
}

template <typename I>
void CreatePrimaryRequest<I>::refresh_image() {
  if (!m_image_ctx->state->is_refresh_required()) {
    get_mirror_image();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void CreatePrimaryRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_image();
}

template <typename I>
void CreatePrimaryRequest<I>::get_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx->id);

  librados::AioCompletion *comp = create_rados_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_get_mirror_image>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreatePrimaryRequest<I>::handle_get_mirror_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  cls::rbd::MirrorImage mirror_image;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (mirror_image.mode != cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT ||
      mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    lderr(cct) << "snapshot based mirroring is not enabled" << dendl;
    finish(-EINVAL);
    return;
  }

  if (!validate_snapshot()) {
    finish(-EINVAL);
    return;
  }

  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_snap_name = ".mirror.primary." + mirror_image.global_image_id + "." +
    uuid_gen.to_string();

  get_mirror_peers();
}

template <typename I>
void CreatePrimaryRequest<I>::get_mirror_peers() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    CreatePrimaryRequest<I>, &CreatePrimaryRequest<I>::handle_get_mirror_peers>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreatePrimaryRequest<I>::handle_get_mirror_peers(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_peer_list_finish(&iter, &peers);
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve mirror peers: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  for (auto &peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }
    m_mirror_peers.insert(peer.uuid);
  }

  if (m_mirror_peers.empty()) {
    lderr(cct) << "no mirror tx peers configured for the pool" << dendl;
    finish(-EINVAL);
    return;
  }

  create_snapshot();
}

template <typename I>
void CreatePrimaryRequest<I>::create_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  cls::rbd::MirrorPrimarySnapshotNamespace ns{m_demoted, m_mirror_peers};
  auto ctx = create_context_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_create_snapshot>(this);
  m_image_ctx->operations->snap_create(ns, m_snap_name, ctx);
}

template <typename I>
void CreatePrimaryRequest<I>::handle_create_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create mirror snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_snap_id != nullptr) {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    cls::rbd::MirrorPrimarySnapshotNamespace ns{m_demoted, m_mirror_peers};
    *m_snap_id = m_image_ctx->get_snap_id(ns, m_snap_name);
  }

  unlink_peer();
}

template <typename I>
void CreatePrimaryRequest<I>::unlink_peer() {
  uint64_t max_snapshots = m_image_ctx->config.template get_val<uint64_t>(
    "rbd_mirroring_max_mirroring_snapshots");
  ceph_assert(max_snapshots >= 3);

  std::string peer_uuid;
  uint64_t snap_id = CEPH_NOSNAP;

  for (auto &peer : m_mirror_peers) {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    size_t count = 0;
    uint64_t prev_snap_id = 0;
    for (auto &snap_it : m_image_ctx->snap_info) {
      if (boost::get<cls::rbd::MirrorNonPrimarySnapshotNamespace>(
            &snap_it.second.snap_namespace)) {
        break;
      }
      auto info = boost::get<cls::rbd::MirrorPrimarySnapshotNamespace>(
        &snap_it.second.snap_namespace);
      if (info == nullptr) {
        continue;
      }
      if (info->demoted) {
        break;
      }
      if (info->mirror_peers.count(peer) == 0) {
        continue;
      }
      count++;
      if (count == max_snapshots - 1) {
        prev_snap_id = snap_it.first;
      } else if (count > max_snapshots) {
        peer_uuid = peer;
        snap_id = prev_snap_id;
        break;
      }
    }
    if (snap_id != CEPH_NOSNAP) {
      break;
    }
  }

  if (snap_id == CEPH_NOSNAP) {
    finish(0);
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "peer=" << peer_uuid << ", snap_id=" << snap_id << dendl;

  auto ctx = create_context_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_unlink_peer>(this);
  auto req = UnlinkPeerRequest<I>::create(m_image_ctx, snap_id, peer_uuid, ctx);
  req->send();
}

template <typename I>
void CreatePrimaryRequest<I>::handle_unlink_peer(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to unlink peer: " << cpp_strerror(r) << dendl;
    finish(0); // not fatal
    return;
  }

  unlink_peer();
}

template <typename I>
void CreatePrimaryRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
bool CreatePrimaryRequest<I>::validate_snapshot() {
  CephContext *cct = m_image_ctx->cct;

  std::shared_lock image_locker{m_image_ctx->image_lock};

  for (auto it = m_image_ctx->snap_info.rbegin();
       it != m_image_ctx->snap_info.rend(); it++) {
    auto non_primary = boost::get<cls::rbd::MirrorNonPrimarySnapshotNamespace>(
      &it->second.snap_namespace);
    if (non_primary != nullptr) {
      ldout(cct, 20) << "previous mirror snapshot snap_id=" << it->first << " "
                     << *non_primary << dendl;
      if (!m_force) {
        lderr(cct) << "trying to create primary snapshot without force "
                   << "when previous snapshot is non-primary"
                   << dendl;
        return false;
      }
      if (m_demoted) {
        lderr(cct) << "trying to create primary demoted snapshot "
                   << "when previous snapshot is non-primary"
                   << dendl;
        return false;
      }
      if (!non_primary->copied) {
        lderr(cct) << "trying to create primary snapshot "
                   << "when previous non-primary snapshot is not copied yet"
                   << dendl;
        return false;
      }
      return true;
    }
    auto primary = boost::get<cls::rbd::MirrorPrimarySnapshotNamespace>(
      &it->second.snap_namespace);
    if (primary == nullptr) {
      continue;
    }
    ldout(cct, 20) << "previous snapshot snap_id=" << it->first << " "
                   << *primary << dendl;
    if (primary->demoted && !m_force) {
      lderr(cct) << "trying to create primary snapshot without force "
                 << "when previous primary snapshot is demoted"
                 << dendl;
      return false;
    }
    return true;
  }

  ldout(cct, 20) << "no previous mirror snapshots found" << dendl;
  return true;
}

} // namespace mirror_snapshot
} // namespace librbd

template class librbd::mirror_snapshot::CreatePrimaryRequest<librbd::ImageCtx>;
