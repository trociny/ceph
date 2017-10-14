// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Migrate.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/SetHeadRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/internal.h"
#include "librbd/io/ImageRequestWQ.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Migrate: "

namespace librbd {
namespace api {

template <typename I>
int Migrate<I>::migrate(I *ictx, librados::IoCtx& dst_io_ctx,
                        const char *dstname_, ImageOptions& opts,
                        ProgressContext &prog_ctx) {
  CephContext* cct = ictx->cct;

  std::string dstname = (dstname_ != nullptr && *dstname_ != '\0') ?
    dstname_ : ictx->name.c_str();

  ldout(cct, 10) << "migrate " << ictx->md_ctx.get_pool_name() << "/"
                 << ictx->name << " -> " << dst_io_ctx.get_pool_name()
                 << "/" << dstname << ", opts=" << opts << dendl;

  if (ictx->read_only) {
    return -EROFS;
  }

  std::list<obj_watch_t> watchers;
  int r = ictx->md_ctx.list_watchers(ictx->header_oid, &watchers);
  if (watchers.size() > 1) {
    // TODO: filter out mirror watchers
    lderr(cct) << "image has watchers - not migrating" << dendl;
    return -EBUSY;
  }

  uint64_t features;
  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    if (ictx->snaps.size() > 0) {
      // XXXMG: or rather check for children?
      lderr(cct) << "image has snapshots - not migrating" << dendl;
      return r;
    }
    features = ictx->features;
  }

  uint64_t format = 2;
  if (opts.get(RBD_IMAGE_OPTION_FORMAT, &format) != 0) {
    opts.set(RBD_IMAGE_OPTION_FORMAT, format);
  }

  if (format != 2) {
    lderr(cct) << "unsupported destination image format: " << format << dendl;
    return -EINVAL;
  }

  uint64_t stripe_unit = ictx->stripe_unit;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
  }
  uint64_t stripe_count = ictx->stripe_count;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
  }
  uint64_t order = ictx->order;
  if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
    opts.set(RBD_IMAGE_OPTION_ORDER, order);
  }
  if (opts.get(RBD_IMAGE_OPTION_FEATURES, &features) != 0) {
    opts.set(RBD_IMAGE_OPTION_FEATURES, features);
  }
  if (features & ~RBD_FEATURES_ALL) {
    lderr(cct) << "librbd does not support requested features" << dendl;
    return -ENOSYS;
  }

  return Migrate(ictx, dst_io_ctx, dstname, opts, prog_ctx).execute();
}

template <typename I>
Migrate<I>::Migrate(I *image_ctx, librados::IoCtx& dst_io_ctx,
                    const std::string &dstname, ImageOptions& opts,
                    ProgressContext &prog_ctx)
  : m_cct(static_cast<CephContext *>(dst_io_ctx.cct())),
    m_image_ctx(image_ctx), m_dst_io_ctx(dst_io_ctx), m_dstname(dstname),
    m_image_options(opts), m_prog_ctx(prog_ctx) {
  m_src_io_ctx.dup(image_ctx->md_ctx);
}

template <typename I>
int Migrate<I>::execute() {
  int r;
  std::string src_name;
  std::string src_id;
  std::string src_header_oid;
  bool old_format = m_image_ctx->old_format;

  if (old_format) {
    src_name = ".migrate." + m_image_ctx->name; // XXXMG
    src_header_oid = util::old_header_name(src_name);

    r = tmap_rm(m_src_io_ctx, m_image_ctx->name);
    if (r < 0) {
      lderr(m_cct) << "failed removing " << m_image_ctx->name
                   << " from tmap: " << cpp_strerror(r) << dendl;
      return r;
    }


    ldout(m_cct, 20) << "moving " << m_image_ctx->header_oid << " -> "
                     << src_header_oid << dendl;

    bufferlist header;
    r = read_header_bl(m_src_io_ctx, m_image_ctx->header_oid, header, nullptr);
    if (r < 0) {
      lderr(m_cct) << "failed reading " << m_image_ctx->header_oid
                   << " header: " << cpp_strerror(r) << dendl;
      return r;
    }

    r = m_src_io_ctx.write(src_header_oid, header, header.length(), 0);
    if (r < 0) {
      lderr(m_cct) << "failed writing " << src_header_oid
                   << " header: " << cpp_strerror(r) << dendl;
      return r;
    }

    r = m_src_io_ctx.remove(m_image_ctx->header_oid);
    if (r < 0) {
      lderr(m_cct) << "failed removing " << m_image_ctx->header_oid
                   << " header: " << cpp_strerror(r) << dendl;
      return r;
    }
  } else {
    src_id = m_image_ctx->id;
    src_header_oid = m_image_ctx->header_oid;

    m_image_ctx->owner_lock.get_read();
    if (m_image_ctx->exclusive_lock != nullptr &&
        m_image_ctx->exclusive_lock->is_lock_owner()) {
      C_SaferCond ctx;
      m_image_ctx->exclusive_lock->shut_down(&ctx);
      m_image_ctx->owner_lock.put_read();
      r = ctx.wait();
      if (r < 0) {
        lderr(m_cct) << "error shutting down exclusive lock: "
                     << cpp_strerror(r) << dendl;
        return r;
      }
    } else {
      m_image_ctx->owner_lock.put_read();
    }

    r = trash_move(m_src_io_ctx, RBD_TRASH_IMAGE_SOURCE_USER, m_image_ctx->name,
                   0 /* XXXMG: use some reasonable expiration time? */);
    if (r < 0) {
      lderr(m_cct) << "failed moving image to trash: " << cpp_strerror(r)
                   << dendl;
      return r;
    }
  }

  std::string image_id = util::generate_image_id(m_dst_io_ctx);

  r = create(m_dst_io_ctx, m_dstname.c_str(), image_id, m_image_ctx->size,
             m_image_options, "", "", false);
  if (r < 0) {
    lderr(m_cct) << "header creation failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto dst_image_ctx = new librbd::ImageCtx(m_dstname, image_id, nullptr,
                                            m_dst_io_ctx, false);
  r = dst_image_ctx->state->open(false);
  if (r < 0) {
    lderr(m_cct) << "failed to open newly created header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  SnapSeqs snap_seqs;

  C_SaferCond on_snapshot_copy;
  auto snapshot_copy_req = librbd::deep_copy::SnapshotCopyRequest<I>::create(
      m_image_ctx, dst_image_ctx, m_image_ctx->op_work_queue, &snap_seqs,
      &on_snapshot_copy);
  snapshot_copy_req->send();
  r = on_snapshot_copy.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to copy snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond on_set_head;
  uint64_t size = m_image_ctx->size; // XXXMG: snap lock
  librbd::ParentSpec parent_spec = {}; // XXXMG
  uint64_t parent_overlap = 0; // XXXMG
  auto set_head_req = librbd::deep_copy::SetHeadRequest<I>::create(
    dst_image_ctx, size, parent_spec, parent_overlap, &on_set_head);
  set_head_req->send();
  r = on_set_head.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to set image head: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond on_dst_close;
  dst_image_ctx->state->close(&on_dst_close);
  r = on_dst_close.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to close image: " << cpp_strerror(r) << dendl;
    return r;
  }

  // XXXMG: we set src migrate header so late, because otherwise
  // trash_move above would fail.

  cls::rbd::MigrateSpec migrate_spec =
    {cls::rbd::MIGRATE_TYPE_SRC, m_dst_io_ctx.get_id(), m_dstname, image_id, {}};
  r = cls_client::migrate_set(&m_src_io_ctx, src_header_oid, migrate_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migrate header: " << cpp_strerror(r)
                 << dendl;
    if (r == -EOPNOTSUPP) {
      // XXXMG: rollback
    }
    return r;
  }

  migrate_spec = {cls::rbd::MIGRATE_TYPE_DST, m_src_io_ctx.get_id(), src_name,
                  src_id, snap_seqs};
  r = cls_client::migrate_set(&m_dst_io_ctx, util::header_name(image_id),
                              migrate_spec);
  if (r != 0) {
    lderr(m_cct) << "failed to set migrate header: " << cpp_strerror(r)
                 << dendl;
    if (r == -EOPNOTSUPP) {
      // XXXMG: rollback
    }
    return r;
  }

  C_SaferCond on_close;
  m_image_ctx->state->close(&on_close);
  r = on_close.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to close image: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_image_ctx->clear_nonreusable();

  m_image_ctx->cct = m_cct;

  ThreadPool *thread_pool;
  m_image_ctx->get_thread_pool_instance(m_cct, &thread_pool,
                                        &m_image_ctx->op_work_queue);

  assert(m_image_ctx->io_work_queue == nullptr);
  m_image_ctx->io_work_queue = new io::ImageRequestWQ<I>(
      m_image_ctx, "librbd::io_work_queue",
      m_cct->_conf->get_val<int64_t>("rbd_op_thread_timeout"), thread_pool);

  assert(m_image_ctx->state == nullptr);
  m_image_ctx->state = new ImageState<I>(m_image_ctx);

  m_image_ctx->name = m_dstname;
  m_image_ctx->id = image_id;
  m_image_ctx->md_ctx = m_dst_io_ctx;

  C_SaferCond on_open;
  m_image_ctx->state->open(false, &on_open);
  r = on_open.wait();
  if (r < 0) {
    lderr(m_cct) << "failed opening newly created image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  r = m_image_ctx->operations->flatten(m_prog_ctx);
  if (r < 0) {
    lderr(m_cct) << "flatten failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (old_format) {
    ldout(m_cct, 20) << "removing " << src_name << dendl;
    librbd::NoOpProgressContext noopprog_ctx;
    int remove_r = remove(m_src_io_ctx, src_name, "", noopprog_ctx, false,
                          false);
    // XXXMG: Normally it will remove the image but will return -ENOENT
    // due to tmap_rm failure at the end.
    if (remove_r < 0 && remove_r != -ENOENT) {
      lderr(m_cct) << "failed removing source image: " << cpp_strerror(remove_r)
                   << dendl;
    }
  } else {
    int remove_r = cls_client::migrate_remove(&m_src_io_ctx,
                                              util::header_name(src_id));
    if (remove_r < 0) {
      lderr(m_cct) << "failed removing migrate header: "
                   << cpp_strerror(remove_r) << dendl;
    }
  }

  r = cls_client::migrate_remove(&m_dst_io_ctx, util::header_name(image_id));
  if (r < 0) {
    lderr(m_cct) << "failed removing migrate header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Migrate<librbd::ImageCtx>;
