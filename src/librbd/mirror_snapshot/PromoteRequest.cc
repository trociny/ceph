// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror_snapshot/PromoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror_snapshot/CreatePrimaryRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror_snapshot::PromoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror_snapshot {

using librbd::util::create_context_callback;

template <typename I>
void PromoteRequest<I>::send() {
  refresh_image();
}

template <typename I>
void PromoteRequest<I>::refresh_image() {
  if (!m_image_ctx->state->is_refresh_required()) {
    create_snapshot();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void PromoteRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_snapshot();
}

template <typename I>
void PromoteRequest<I>::create_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_create_snapshot>(this);

  auto req = CreatePrimaryRequest<I>::create(m_image_ctx, false, true, nullptr,
                                             ctx);
  req->send();
}

template <typename I>
void PromoteRequest<I>::handle_create_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create mirror snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void PromoteRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror_snapshot
} // namespace librbd

template class librbd::mirror_snapshot::PromoteRequest<librbd::ImageCtx>;
