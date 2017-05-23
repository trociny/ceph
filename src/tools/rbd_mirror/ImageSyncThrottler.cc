// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ImageSyncThrottler.h"
#include "ImageSync.h"
#include "InstanceWatcher.h"
#include "common/ceph_context.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSyncThrottler:: " << this \
                           << " " << __func__ << ": "
namespace rbd {
namespace mirror {

template <typename ImageCtxT>
struct ImageSyncThrottler<ImageCtxT>::C_SyncHolder : public Context {
  ImageSyncThrottler<ImageCtxT> *m_sync_throttler;
  std::string m_local_image_id;
  ImageSync<ImageCtxT> *m_sync = nullptr;
  Context *m_on_finish;

  C_SyncHolder(ImageSyncThrottler<ImageCtxT> *sync_throttler,
               const std::string &local_image_id, Context *on_finish)
    : m_sync_throttler(sync_throttler),
      m_local_image_id(local_image_id), m_on_finish(on_finish) {
  }

  void finish(int r) override {
    m_sync_throttler->handle_sync_finished(r, this);
  }
};

template <typename I>
ImageSyncThrottler<I>::ImageSyncThrottler()
  : m_lock(librbd::util::unique_lock_name("rbd::mirror::ImageSyncThrottler",
                                          this)) {
}

template <typename I>
ImageSyncThrottler<I>::~ImageSyncThrottler() {
  Mutex::Locker locker(m_lock);
  assert(m_inflight_syncs.empty());
}

template <typename I>
void ImageSyncThrottler<I>::init(InstanceWatcher<I> *instance_watcher) {
  dout(20) << "instance_watcher=" << instance_watcher << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_instance_watcher == nullptr);

  m_instance_watcher = instance_watcher;
}

template <typename I>
void ImageSyncThrottler<I>::start_sync(I *local_image_ctx, I *remote_image_ctx,
                                       SafeTimer *timer, Mutex *timer_lock,
                                       const std::string &mirror_uuid,
                                       Journaler *journaler,
                                       MirrorPeerClientMeta *client_meta,
                                       ContextWQ *work_queue,
                                       Context *on_finish,
                                       ProgressContext *progress_ctx) {
  dout(20) << "local_image_id=" << local_image_ctx->id << dendl;

  C_SyncHolder *sync_holder = new C_SyncHolder(this, local_image_ctx->id,
                                               on_finish);
  sync_holder->m_sync = ImageSync<I>::create(local_image_ctx, remote_image_ctx,
                                             timer, timer_lock, mirror_uuid,
                                             journaler, client_meta, work_queue,
                                             sync_holder, progress_ctx);
  sync_holder->m_sync->get();

  auto on_start = new FunctionContext(
    [this, sync_holder] (int r) {
      handle_sync_started(r, sync_holder);
    });

  m_instance_watcher->notify_sync_request(local_image_ctx->id, on_start);
}

template <typename I>
void ImageSyncThrottler<I>::cancel_sync(const std::string &local_image_id) {
  dout(20) << "local_image_id=" << local_image_id << dendl;

  if (m_instance_watcher->cancel_notify_sync_request(local_image_id)) {
    return;
  }

  C_SyncHolder *sync_holder = nullptr;

  {
    Mutex::Locker locker(m_lock);
    auto it = m_inflight_syncs.find(local_image_id);
    if (it != m_inflight_syncs.end()) {
      sync_holder = it->second;
    }
  }

  if (sync_holder != nullptr) {
    dout(10) << "canceled running image sync for local_image_id "
             << sync_holder->m_local_image_id << dendl;
    sync_holder->m_sync->cancel();
  }
}

template <typename I>
void ImageSyncThrottler<I>::print_status(Formatter *f, stringstream *ss) {
  m_instance_watcher->print_sync_status(f, ss);
}

template <typename I>
void ImageSyncThrottler<I>::handle_sync_started(int r,
                                                C_SyncHolder *sync_holder) {
  dout(20) << "local_image_id=" << sync_holder->m_local_image_id << ", r=" << r
           << dendl;

  if (r == -ECANCELED) {
    dout(10) << "canceled waiting image sync for local_image_id "
             << sync_holder->m_local_image_id << dendl;
    sync_holder->m_on_finish->complete(r);
    sync_holder->m_sync->put();
    delete sync_holder;
    return;
  }

  assert(r == 0);

  Mutex::Locker locker(m_lock);
  m_inflight_syncs[sync_holder->m_local_image_id] = sync_holder;
  sync_holder->m_sync->send();
}

template <typename I>
void ImageSyncThrottler<I>::handle_sync_finished(int r,
                                                 C_SyncHolder *sync_holder) {
  dout(20) << "local_image_id=" << sync_holder->m_local_image_id << ", r=" << r
           << dendl;

  sync_holder->m_on_finish->complete(r);
  m_instance_watcher->notify_sync_complete(sync_holder->m_local_image_id);
  Mutex::Locker locker(m_lock);
  auto resut = m_inflight_syncs.erase(sync_holder->m_local_image_id);
  assert(resut > 0);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSyncThrottler<librbd::ImageCtx>;
