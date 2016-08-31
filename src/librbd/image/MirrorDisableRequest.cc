// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/MirrorDisableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/journal/cls_journal_client.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::MirrorDisableRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;

template <typename I>
MirrorDisableRequest<I>::MirrorDisableRequest(I *image_ctx, bool force,
                                              bool remove,  Context *on_finish)
  : m_image_ctx(image_ctx), m_force(force), m_remove(remove),
    m_on_finish(on_finish), m_lock("MirrorDisableRequest::m_lock") {
}

template <typename I>
void MirrorDisableRequest<I>::send() {
  send_get_mirror_image();
}

template <typename I>
void MirrorDisableRequest<I>::send_get_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx->id);

  using klass = MirrorDisableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_get_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_get_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    *result = cls_client::mirror_image_get_finish(&iter, &m_mirror_image);
  }

  if (*result < 0) {
    if (*result == -ENOENT) {
      ldout(cct, 20) << this << " " << __func__
		     << ": mirroring is not enabled for this image" << dendl;
      *result = 0;
    } else if (*result == -EOPNOTSUPP) {
      ldout(cct, 5) << this << " " << __func__
		    << ": mirroring is not supported by OSD" << dendl;
    } else {
      lderr(cct) << "failed to retreive mirror image: " << cpp_strerror(*result)
		 << dendl;
    }
    return m_on_finish;
  }

  send_get_tag_owner();
  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_get_tag_owner() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = MirrorDisableRequest<I>;
  Context *ctx = util::create_context_callback<
      klass, &klass::handle_get_tag_owner>(this);

  Journal<>::is_tag_owner(m_image_ctx, &m_is_primary, ctx);
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_get_tag_owner(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to check tag ownership: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  if (!m_is_primary && !m_force) {
    lderr(cct) << "mirrored image is not primary, "
	       << "add force option to disable mirroring" << dendl;
    *result = -EINVAL;
    return m_on_finish;
  }

  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  send_set_mirror_image();
  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_set_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_set(&op, m_image_ctx->id, m_mirror_image);

  using klass = MirrorDisableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_set_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_set_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to disable mirroring: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_notify_mirroring_watcher();
  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_notify_mirroring_watcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = MirrorDisableRequest<I>;
  Context *ctx = util::create_context_callback<
    klass, &klass::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<>::notify_image_updated(m_image_ctx->md_ctx,
					   cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
					   m_image_ctx->id,
					   m_mirror_image.global_image_id, ctx);
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_notify_mirroring_watcher(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to send update notification: "
	       << cpp_strerror(*result) << dendl;
    *result = 0;
  }

  send_get_clients();
  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_get_clients() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = MirrorDisableRequest<I>;
  Context *ctx = util::create_context_callback<
    klass, &klass::handle_get_clients>(this);

  std::string header_oid = ::journal::Journaler::header_oid(m_image_ctx->id);
  m_clients.clear();
  cls::journal::client::client_list(m_image_ctx->md_ctx, header_oid, &m_clients,
                                    ctx);
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_get_clients(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to get registered clients: " << cpp_strerror(*result)
	       << dendl;
    return m_on_finish;
  }

  Mutex::Locker locker(m_lock);

  assert(m_current_ops.empty());

  for (auto client : m_clients) {
    journal::ClientData client_data;
    bufferlist::iterator bl_it = client.data.begin();
    try {
      ::decode(client_data, bl_it);
    } catch (const buffer::error &err) {
      lderr(cct) << "failed to decode client data" << dendl;
      m_error_result = -EBADMSG;
      continue;
    }

    journal::ClientMetaType type = client_data.get_client_meta_type();
    if (type != journal::ClientMetaType::MIRROR_PEER_CLIENT_META_TYPE) {
      continue;
    }

    if (m_current_ops.find(client.id) != m_current_ops.end()) {
      // Should not happen.
      lderr(cct) << this << " " << __func__ << ": clients with the same id "
		 << client.id << dendl;
      continue;
    }

    m_current_ops[client.id] = 0;
    m_ret[client.id] = 0;

    journal::MirrorPeerClientMeta client_meta =
      boost::get<journal::MirrorPeerClientMeta>(client_data.client_meta);

    for (const auto& sync : client_meta.sync_points) {
      send_remove_snap(client.id, sync.snap_name);
    }

    if (m_current_ops[client.id] == 0) {
      // no snaps to remove
      send_unregister_client(client.id);
    }
  }

  if (m_current_ops.empty()) {
    if (m_error_result < 0) {
      *result = m_error_result;
      return m_on_finish;
    }
    // no mirror clients to unregister
    send_remove_mirror_image();
  }

  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_remove_snap(const std::string &client_id,
                                               const std::string &snap_name) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": client_id=" << client_id
                 << ", snap_name=" << snap_name << dendl;

  assert(m_lock.is_locked());

  m_current_ops[client_id]++;

  Context *ctx = create_context_callback(
    &MirrorDisableRequest<I>::handle_remove_snap, client_id);

  m_image_ctx->operations->execute_snap_remove(snap_name.c_str(), ctx);
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_remove_snap(int *result,
    const std::string &client_id) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_current_ops[client_id] > 0);
  m_current_ops[client_id]--;

  if (*result < 0 && *result != ENOENT) {
    lderr(cct) <<
      "failed to remove temporary snapshot created by remote peer: "
	       << cpp_strerror(*result) << dendl;
    m_ret[client_id] = *result;
  }

  if (m_current_ops[client_id] == 0) {
    send_unregister_client(client_id);
  }

  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_unregister_client(
  const std::string &client_id) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());
  assert(m_current_ops[client_id] == 0);

  Context *ctx = create_context_callback(
    &MirrorDisableRequest<I>::handle_unregister_client, client_id);

  if (m_ret[client_id] < 0) {
    ctx->complete(m_ret[client_id]);
    return;
  }

  librados::ObjectWriteOperation op;
  cls::journal::client::client_unregister(&op, client_id);
  std::string header_oid = ::journal::Journaler::header_oid(m_image_ctx->id);
  librados::AioCompletion *comp = create_rados_ack_callback(ctx);

  int r = m_image_ctx->md_ctx.aio_operate(header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_unregister_client(
  int *result, const std::string &client_id) {

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_current_ops[client_id] == 0);
  m_current_ops.erase(client_id);

  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to unregister remote journal client: "
	       << cpp_strerror(*result) << dendl;
    m_error_result = *result;
  }

  if (!m_current_ops.empty()) {
    return nullptr;
  }

  if (m_error_result < 0) {
    *result = m_error_result;
    return m_on_finish;
  }

  if (!m_remove) {
    return m_on_finish;
  }

  send_get_clients();
  return nullptr;
}

template <typename I>
void MirrorDisableRequest<I>::send_remove_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_remove(&op, m_image_ctx->id);

  using klass = MirrorDisableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_remove_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *MirrorDisableRequest<I>::handle_remove_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -ENOENT) {
    *result = 0;
  }

  if (*result < 0) {
    lderr(cct) << "failed to remove mirror image: " << cpp_strerror(*result)
	       << dendl;
  }

  ldout(cct, 20) << this << " " << __func__
		 <<  ": removed image state from rbd_mirroring object" << dendl;

  if (m_is_primary) {
    // TODO: send notification to mirroring object about update
  }

  return m_on_finish;
}

template <typename I>
Context *MirrorDisableRequest<I>::create_context_callback(
  Context*(MirrorDisableRequest<I>::*handle)(int*, const std::string &client_id),
  const std::string &client_id) {

  return new FunctionContext([this, handle, client_id](int r) {
      Context *on_finish = (this->*handle)(&r, client_id);
      if (on_finish != nullptr) {
	on_finish->complete(r);
	delete this;
      }
    });
}

} // namespace image
} // namespace librbd

template class librbd::image::MirrorDisableRequest<librbd::ImageCtx>;
