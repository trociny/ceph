// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/EnableFeaturesRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/image/CreateObjectMapRequest.h"
#include "librbd/image/MirrorEnableRequest.h"
#include "librbd/image/SetFlagsRequest.h"
#include "librbd/journal/CreateRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::EnableFeaturesRequest: "

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_rados_ack_callback;

template <typename I>
EnableFeaturesRequest<I>::EnableFeaturesRequest(I &image_ctx,
                                               Context *on_finish,
                                               uint64_t features)
  : Request<I>(image_ctx, on_finish), m_features(features) {
}

template <typename I>
void EnableFeaturesRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  assert(image_ctx.owner_lock.is_locked());

  ldout(cct, 20) << this << " " << __func__ << ": features=" << m_features
		 << dendl;

  if (this->is_canceled()) {
    this->async_complete(-ERESTART);
  } else {
    send_get_mirror_mode();
  }
}

template <typename I>
bool EnableFeaturesRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void EnableFeaturesRequest<I>::send_get_mirror_mode() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if ((m_features & RBD_FEATURE_JOURNALING) == 0) {
    send_block_writes();
    return;
  }

  ldout(cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_mode_get_start(&op);

  using klass = EnableFeaturesRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_get_mirror_mode>(this);
  m_out_bl.clear();
  int r = image_ctx.md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_get_mirror_mode(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  cls::rbd::MirrorMode mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::mirror_mode_get_finish(&it, &mirror_mode);
  }

  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to retrieve pool mirror mode: "
               << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }

  m_enable_mirroring = (mirror_mode == cls::rbd::MIRROR_MODE_POOL);

  send_block_writes();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_block_writes() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  image_ctx.owner_lock.get_read();

  image_ctx.aio_work_queue->block_writes(create_context_callback<
    EnableFeaturesRequest<I>,
    &EnableFeaturesRequest<I>::handle_block_writes>(this));
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_block_writes(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    image_ctx.aio_work_queue->unblock_writes();
    return this->create_context_finisher(*result);
  }

  // avoid accepting new requests from peers while we manipulate
  // the image features
  if (image_ctx.exclusive_lock != nullptr) {
    image_ctx.exclusive_lock->block_requests(0);
  }

  assert(!m_snap_lock_acquired);
  image_ctx.snap_lock.get_write();
  m_snap_lock_acquired = true;

  if ((m_features & image_ctx.features) != 0) {
    lderr(cct) << "one or more requested features are already enabled" << dendl;
    return handle_finish(-EINVAL);
  }
  m_features &= ~image_ctx.features;
  m_new_features = image_ctx.features | m_features;

  if ((m_features & RBD_FEATURE_OBJECT_MAP) != 0) {
    if ((m_new_features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      lderr(cct) << "cannot enable object map" << dendl;
      return handle_finish(-EINVAL);
    }
    m_enable_flags |= RBD_FLAG_OBJECT_MAP_INVALID;
    m_features_mask |= RBD_FEATURE_EXCLUSIVE_LOCK;
  }
  if ((m_features & RBD_FEATURE_FAST_DIFF) != 0) {
    if ((m_new_features & RBD_FEATURE_OBJECT_MAP) == 0) {
      lderr(cct) << "cannot enable fast diff" << dendl;
      return handle_finish(-EINVAL);
    }
    m_enable_flags |= RBD_FLAG_FAST_DIFF_INVALID;
    m_features_mask |= (RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_EXCLUSIVE_LOCK);
  }
  if ((m_features & RBD_FEATURE_JOURNALING) != 0) {
    if ((m_new_features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      lderr(cct) << "cannot enable journaling" << dendl;
      return handle_finish(-EINVAL);
    }
    m_features_mask |= RBD_FEATURE_EXCLUSIVE_LOCK;

    send_create_journal();
    return nullptr;
  }

  send_append_op_event();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_create_journal() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << dendl;

  journal::TagData tag_data(librbd::Journal<>::LOCAL_MIRROR_UUID);
  Context *ctx = create_context_callback<
    EnableFeaturesRequest<I>,
    &EnableFeaturesRequest<I>::handle_create_journal>(this);

  journal::CreateRequest<I> *req = journal::CreateRequest<I>::create(
    image_ctx.md_ctx, image_ctx.id, image_ctx.journal_order,
    image_ctx.journal_splay_width, image_ctx.journal_pool,
    cls::journal::Tag::TAG_CLASS_NEW, tag_data,
    librbd::Journal<>::IMAGE_CLIENT_ID, image_ctx.op_work_queue, ctx);

  req->send();
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_create_journal(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to create journal: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_append_op_event();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_append_op_event() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (!this->template append_op_event<
      EnableFeaturesRequest<I>,
      &EnableFeaturesRequest<I>::handle_append_op_event>(this)) {
    send_update_flags();
  }

  ldout(cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_append_op_event(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to commit journal entry: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_update_flags();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_update_flags() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": enable_flags="
		 << m_enable_flags << dendl;

  if (m_enable_flags == 0) {
    send_set_features();
    return;
  }

  Context *ctx = create_context_callback<
    EnableFeaturesRequest<I>,
    &EnableFeaturesRequest<I>::handle_update_flags>(this);

  image::SetFlagsRequest<I> *req =
    image::SetFlagsRequest<I>::create(&image_ctx, m_enable_flags,
                                      m_enable_flags, ctx);
  req->send();
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_update_flags(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to update image flags: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_set_features();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_set_features() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": new_features="
		 << m_new_features << ", features_mask=" << m_features_mask
		 << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::set_features(&op, m_new_features, m_features_mask);

  using klass = EnableFeaturesRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_update_flags>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_set_features(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to update features: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_create_object_map();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_create_object_map() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  if (((image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0) ||
      ((m_features & RBD_FEATURE_OBJECT_MAP) == 0)) {
    send_enable_mirror_image();
    return;
  }

  Context *ctx = create_context_callback<
    EnableFeaturesRequest<I>,
    &EnableFeaturesRequest<I>::handle_create_object_map>(this);

  image::CreateObjectMapRequest<I> *req =
    image::CreateObjectMapRequest<I>::create(&image_ctx, ctx);
  req->send();
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_create_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to create object map: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_enable_mirror_image();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_enable_mirror_image() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  if (!m_enable_mirroring) {
    send_notify_update();
    return;
  }

  Context *ctx = create_context_callback<
    EnableFeaturesRequest<I>,
    &EnableFeaturesRequest<I>::handle_enable_mirror_image>(this);

  image::MirrorEnableRequest<I> *req =
    image::MirrorEnableRequest<I>::create(&image_ctx.md_ctx, image_ctx.id, ctx);
  req->send();
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_enable_mirror_image(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to enable mirroring: " << cpp_strerror(*result)
               << dendl;
    // not fatal
  }

  send_notify_update();
  return nullptr;
}

template <typename I>
void EnableFeaturesRequest<I>::send_notify_update() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  image_ctx.snap_lock.put_write();
  m_snap_lock_acquired = false;

  Context *ctx = create_context_callback<
    EnableFeaturesRequest<I>,
    &EnableFeaturesRequest<I>::handle_notify_update>(this);

  image_ctx.notify_update(ctx);
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_notify_update(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  return handle_finish(*result);
}

template <typename I>
Context *EnableFeaturesRequest<I>::handle_finish(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  if (m_snap_lock_acquired) {
    m_snap_lock_acquired = false;
    image_ctx.snap_lock.put_write();
  }
  if (image_ctx.exclusive_lock != nullptr) {
    image_ctx.exclusive_lock->unblock_requests();
  }
  image_ctx.aio_work_queue->unblock_writes();
  image_ctx.owner_lock.put_read();

  return this->create_context_finisher(r);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::EnableFeaturesRequest<librbd::ImageCtx>;
