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
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/internal.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/mirror/EnableRequest.h"

#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Migrate: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

int trash_search(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                 const std::string &image_name, std::string *image_id) {
  std::vector<trash_image_info_t> entries;

  int r = trash_list(io_ctx, entries);
  if (r < 0) {
    return r;
  }

  for (auto &entry : entries) {
    if (entry.source == source && entry.name == image_name) {
      *image_id = entry.id;
      return 0;
    }
  }

  return -ENOENT;
}

} // anonymous namespace

template <typename I>
int Migrate<I>::migrate(librados::IoCtx& io_ctx, const std::string &image_name,
                        librados::IoCtx& dest_io_ctx,
                        const std::string &dest_image_name_, ImageOptions& opts,
                        ProgressContext &prog_ctx) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  std::string dest_image_name = dest_image_name_.empty() ? image_name :
    dest_image_name_;

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << " -> "
                 << dest_io_ctx.get_pool_name() << "/" << dest_image_name
                 << ", opts=" << opts << dendl;

  auto image_ctx = I::create(image_name, "", nullptr, io_ctx, false);
  int r = image_ctx->state->open(0);
  if (r < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
    return r;
  }
  BOOST_SCOPE_EXIT_TPL(&r, image_ctx) {
    if (r < 0) {
      image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  std::list<librbd::WatcherInfo> watchers;
  C_SaferCond on_list_watchers;
  auto list_watchers_request = librbd::image::ListWatchersRequest<I>::create(
      *image_ctx, &watchers, &on_list_watchers);
  list_watchers_request->send();
  r = on_list_watchers.wait();
  if (r < 0) {
    lderr(cct) << "failed listing watchers:" << cpp_strerror(r) << dendl;
    return r;
  }
  auto it = std::find_if(watchers.begin(), watchers.end(),
                         [] (librbd::WatcherInfo &w) {
                           return (!w.me && !w.mirroring);
                         });
  if (it != watchers.end()) {
    lderr(cct) << "image has watchers - not migrating" << dendl;
    return -EBUSY;
  }

  uint64_t format = 2;
  if (opts.get(RBD_IMAGE_OPTION_FORMAT, &format) != 0) {
    opts.set(RBD_IMAGE_OPTION_FORMAT, format);
  }
  if (format != 2) {
    lderr(cct) << "unsupported destination image format: " << format << dendl;
    return -EINVAL;
  }

  uint64_t features;
  {
    RWLock::RLocker snap_locker(image_ctx->snap_lock);
    features = image_ctx->features;
  }
  opts.get(RBD_IMAGE_OPTION_FEATURES, &features);
  if ((features & ~RBD_FEATURES_ALL) != 0) {
    lderr(cct) << "librbd does not support requested features" << dendl;
    return -ENOSYS;
  }
  features |= RBD_FEATURE_MIGRATING;
  opts.set(RBD_IMAGE_OPTION_FEATURES, features);

  uint64_t order = image_ctx->order;
  if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
    opts.set(RBD_IMAGE_OPTION_ORDER, order);
  }
  r = image::CreateRequest<I>::validate_order(cct, order);
  if (r < 0) {
    return r;
  }

  uint64_t stripe_unit = image_ctx->stripe_unit;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
  }
  uint64_t stripe_count = image_ctx->stripe_count;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
  }

  ldout(cct, 20) << "updated opts=" << opts << dendl;

  Migrate migrate(image_ctx, dest_io_ctx, dest_image_name, opts, prog_ctx);

  r = migrate.migrate();

  features &= ~RBD_FEATURE_MIGRATING;
  opts.set(RBD_IMAGE_OPTION_FEATURES, features);

  return r;
}

template <typename I>
int Migrate<I>::abort(librados::IoCtx& io_ctx, const std::string &image_name,
                      ProgressContext &prog_ctx) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  std::string image_id;
  cls::rbd::MigrationSpec migration_spec;
  auto image_ctx = I::create(image_name, "", nullptr, io_ctx, false);

  ldout(cct, 10) << "trying to open image by name " << io_ctx.get_pool_name()
                 << "/" << image_name << dendl;

  int r = image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r < 0) {
    if (r != -ENOENT) {
      lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
      return r;
    }
    image_ctx = nullptr;
  }

  BOOST_SCOPE_EXIT_TPL(&r, &image_ctx) {
    if (r < 0 && image_ctx != nullptr) {
      image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;


  if (r == 0) {
    // The opened image is either a source (then just proceed) or a
    // destination (then look for the source image id in the migration
    // header).

    ldout(cct, 10) << "trying to retrieve migration header" << dendl;

    r = cls_client::migration_get(&image_ctx->md_ctx, image_ctx->header_oid,
                                  &migration_spec);

    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
      image_ctx->state->close();
      return r;
    }

    if (r == 0 && migration_spec.type == cls::rbd::MIGRATION_TYPE_SRC) {
      ldout(cct, 10) << "the source image is opened" << dendl;
    } else {
      ldout(cct, 10) << "the destination image is opened" << dendl;

      // Close and look for the source image.
      int close_r = image_ctx->state->close();
      if (close_r < 0) {
        lderr(cct) << "failed closing image: " << cpp_strerror(close_r)
                   << dendl;
        return close_r;
      }
      image_ctx = nullptr;

      if (r == 0) {
        if (migration_spec.type != cls::rbd::MIGRATION_TYPE_DST) {
          lderr(cct) << "unexpected migration header type: "
                     << migration_spec.type << dendl;
          return -EINVAL;
        }

        if (migration_spec.pool_id != io_ctx.get_id()) {
          lderr(cct) << "unexpected migration header pool id: "
                     << migration_spec.pool_id << dendl;
          return -EINVAL;
        }

        image_id = migration_spec.image_id;

        ldout(cct, 10) << "source image id from migration header:" << image_id
                       << dendl;
      }
    }
  }

  if (r == -ENOENT) {
    ldout(cct, 10) << "source image is not found. Trying trash" << dendl;

    r = trash_search(io_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION, image_name,
                     &image_id);
    if (r < 0) {
      lderr(cct) << "failed to determine image id: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 10) << "source image id from trash: " << image_id << dendl;
  }

  if (image_ctx == nullptr) {
    ldout(cct, 10) << "trying to open image by id " << io_ctx.get_pool_name()
                   << "/" << image_id << dendl;

    assert(!image_id.empty());
    image_ctx = I::create("", image_id, nullptr, io_ctx, false);
    int r = image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
    if (r < 0) {
      lderr(cct) << "failed to open image by id: " << cpp_strerror(r) << dendl;
      image_ctx = nullptr;
      return r;
    }

    ldout(cct, 10) << "trying to retrieve migration header" << dendl;

    r = cls_client::migration_get(&image_ctx->md_ctx, image_ctx->header_oid,
                                  &migration_spec);
    if (r < 0) {
      lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
      return r;
    }
  }

  librados::IoCtx dst_io_ctx;
  bool same_pool = image_ctx->md_ctx.get_id() == migration_spec.pool_id;
  if (!same_pool) {
    r = librados::Rados(image_ctx->md_ctx).ioctx_create2(migration_spec.pool_id,
                                                         dst_io_ctx);
    if (r < 0) {
      lderr(cct) << "error accessing destination pool "
                 << migration_spec.pool_id << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  ldout(cct, 5) << "reverting incomplete migration "
                << image_ctx->md_ctx.get_pool_name() << "/" << image_ctx->name
                << " -> " << (same_pool ? image_ctx->md_ctx.get_pool_name() :
                              dst_io_ctx.get_pool_name()) << "/"
                << migration_spec.image_name << dendl;

  ImageOptions opts;
  Migrate migrate(image_ctx, same_pool ? io_ctx : dst_io_ctx,
                  migration_spec.image_name, opts, prog_ctx);

  r = migrate.abort(migration_spec.image_id, migration_spec.mirroring);

  return r;
}

template <typename I>
Migrate<I>::Migrate(I *src_image_ctx, librados::IoCtx& dst_io_ctx,
                    const std::string &dstname, ImageOptions& opts,
                    ProgressContext &prog_ctx)
  : m_cct(static_cast<CephContext *>(dst_io_ctx.cct())),
    m_src_image_ctx(src_image_ctx), m_dst_io_ctx(dst_io_ctx),
    m_src_old_format(m_src_image_ctx->old_format),
    m_src_image_name(m_src_image_ctx->old_format ? m_src_image_ctx->name : ""),
    m_src_image_id(m_src_image_ctx->id), m_src_header_oid(m_src_image_ctx->header_oid),
    m_dst_image_name(dstname), m_image_options(opts), m_prog_ctx(prog_ctx),
    m_dst_image_id(util::generate_image_id(m_dst_io_ctx)),
    m_src_migration_spec(cls::rbd::MIGRATION_TYPE_SRC,
                         cls::rbd::MIGRATION_STATE_STARTED,
                         m_dst_io_ctx.get_id(), m_dst_image_name,
                         m_dst_image_id, {}, 0, false) {
  m_src_io_ctx.dup(src_image_ctx->md_ctx);
}

template <typename I>
int Migrate<I>::migrate() {
  ldout(m_cct, 10) << dendl;

  int r = list_snaps();
  if (r < 0) {
    return r;
  }

  r = disable_mirroring();
  if (r < 0) {
    return r;
  }

  r = set_migration();
  if (r < 0) {
    return r;
  }

  I *dst_image_ctx;
  r = create_dst_image(&dst_image_ctx);
  if (r < 0) {
    return r;
  }

  // TODO: update group

  BOOST_SCOPE_EXIT_TPL(dst_image_ctx) {
    dst_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  r = dst_image_ctx->operations->migrate(m_prog_ctx);
  if (r < 0) {
    lderr(m_cct) << "migrare failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = unset_migration(dst_image_ctx);
  if (r < 0) {
    return r;
  }

  r = enable_mirroring();
  if (r < 0) {
    return r;
  }

  r = remove_src_image();
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::list_snaps() {
  ldout(m_cct, 10) << dendl;

  int r = snap_list(m_src_image_ctx, m_snaps);
  if (r < 0) {
    lderr(m_cct) << "failed listing snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto &snap : m_snaps) {
    bool is_protected;
    r = snap_is_protected(m_src_image_ctx, snap.name.c_str(), &is_protected);
    if (r < 0) {
      lderr(m_cct) << "failed retrieving snapshot status: " << cpp_strerror(r)
                   << dendl;
      return r;
    }
    if (is_protected) {
      lderr(m_cct) << "image has protected snapshot '" << snap.name << "'"
                   << dendl;
      return -EBUSY;
    }
  }

  return 0;
}

template <typename I>
int Migrate<I>::set_migration() {
  if (m_src_old_format) {
    return v1_set_migration();
  } else {
    return v2_set_migration();
  }
}

template <typename I>
int Migrate<I>::v1_set_migration() {
  ldout(m_cct, 10) << dendl;

  int r = cls_client::migration_set(&m_src_io_ctx, m_src_header_oid,
                                    m_src_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  static_assert(sizeof(RBD_HEADER_TEXT) == sizeof(RBD_MIGRATE_HEADER_TEXT),
                "length of rbd banners must be the same");

  bufferlist header;
  header.append(RBD_MIGRATE_HEADER_TEXT);

  r = m_src_io_ctx.write(m_src_header_oid, header, header.length(), 0);
  if (r < 0) {
    lderr(m_cct) << "failed writing " << m_src_header_oid
                 << " banner: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = tmap_rm(m_src_io_ctx, m_src_image_name);
  if (r < 0) {
    lderr(m_cct) << "failed removing " << m_src_image_name << " from tmap: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::v2_set_migration() {
  ldout(m_cct, 10) << dendl;

  v2_unlink_image();

  int r = cls_client::migration_set(&m_src_io_ctx, m_src_header_oid,
                                    m_src_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migration header: " << cpp_strerror(r)
                 << dendl;
    if (r == -EOPNOTSUPP) {
      v2_relink_image();
    }
    return r;
  }

  r = m_src_image_ctx->operations->update_features(RBD_FEATURE_MIGRATING, true);
  if (r < 0) {
    lderr(m_cct) << "failed to enable migrating feature: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::unset_migration(I *image_ctx) {
  ldout(m_cct, 10) << dendl;

  int r;

  if (image_ctx->old_format) {
    ldout(m_cct, 20) << "old image format => updating image header" << dendl;

    bufferlist header;
    header.append(RBD_HEADER_TEXT);

    r = image_ctx->md_ctx.write(image_ctx->header_oid, header, header.length(),
                                0);
    if (r < 0) {
      lderr(m_cct) << "failed writing " << image_ctx->header_oid
                   << " header: " << cpp_strerror(r) << dendl;
      return r;
    }
  } else {
    ldout(m_cct, 20) << "new image format => disabling migrating feature"
                     << dendl;

    if (image_ctx->test_features(RBD_FEATURE_MIGRATING)) {
      r = image_ctx->operations->update_features(RBD_FEATURE_MIGRATING,
                                                 false);
      if (r < 0) {
        lderr(m_cct) << "failed to disable migrating feature: "
                     << cpp_strerror(r) << dendl;
        return r;
      }
    }
  }

  ldout(m_cct, 20) << "removing migration header" << dendl;

  r = cls_client::migration_remove(&image_ctx->md_ctx, image_ctx->header_oid);
  if (r == -ENOENT) {
    r = 0;
  }
  if (r < 0) {
    lderr(m_cct) << "failed removing migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::v2_unlink_image() {
  ldout(m_cct, 10) << dendl;

  m_src_image_ctx->owner_lock.get_read();
  if (m_src_image_ctx->exclusive_lock != nullptr &&
      m_src_image_ctx->exclusive_lock->is_lock_owner()) {
    C_SaferCond ctx;
    m_src_image_ctx->exclusive_lock->release_lock(&ctx);
    m_src_image_ctx->owner_lock.put_read();
    int r = ctx.wait();
     if (r < 0) {
      lderr(m_cct) << "error releasing exclusive lock: " << cpp_strerror(r)
                   << dendl;
      return r;
     }
  } else {
    m_src_image_ctx->owner_lock.put_read();
  }

  int r = trash_move(m_src_io_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION,
                     m_src_image_ctx->name, 0);
  if (r < 0) {
    lderr(m_cct) << "failed moving image to trash: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::v2_relink_image() {
  int r = trash_restore(m_src_io_ctx, m_src_image_ctx->id, m_src_image_ctx->name);
  if (r < 0) {
    lderr(m_cct) << "failed restoring image from trash: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::create_dst_image(I **dst_image_ctx_) {
  ldout(m_cct, 10) << dendl;

  uint64_t size;
  {
    RWLock::RLocker snap_locker(m_src_image_ctx->snap_lock);
    size = m_src_image_ctx->size;
  }

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(m_cct, &thread_pool, &op_work_queue);

  C_SaferCond on_create;
  auto *req = image::CreateRequest<I>::create(
      m_dst_io_ctx, m_dst_image_name, m_dst_image_id, size, m_image_options, "",
      "", false, op_work_queue, &on_create);
  req->send();
  int r = on_create.wait();
  if (r < 0) {
    lderr(m_cct) << "header creation failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto dst_image_ctx = I::create(m_dst_image_name, m_dst_image_id, nullptr,
                                 m_dst_io_ctx, false);

  r = dst_image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r < 0) {
    lderr(m_cct) << "failed to open newly created header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  BOOST_SCOPE_EXIT_TPL(r, dst_image_ctx) {
    if (r < 0) {
      dst_image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  {
    RWLock::RLocker owner_locker(dst_image_ctx->owner_lock);
    r = dst_image_ctx->operations->prepare_image_update(true);
    if (r < 0) {
      lderr(m_cct) << "cannot obtain exclusive lock" << dendl;
      return r;
    }
    if (dst_image_ctx->exclusive_lock != nullptr) {
      dst_image_ctx->exclusive_lock->block_requests(0);
    }
  }

  SnapSeqs snap_seqs;

  C_SaferCond on_snapshot_copy;
  auto snapshot_copy_req = librbd::deep_copy::SnapshotCopyRequest<I>::create(
      m_src_image_ctx, dst_image_ctx, CEPH_NOSNAP, m_src_image_ctx->op_work_queue,
      &snap_seqs, &on_snapshot_copy);
  snapshot_copy_req->send();
  r = on_snapshot_copy.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to copy snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond on_metadata_copy;
  auto metadata_copy_req = librbd::deep_copy::MetadataCopyRequest<I>::create(
      m_src_image_ctx, dst_image_ctx, &on_metadata_copy);
  metadata_copy_req->send();
  r = on_metadata_copy.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to copy metadata: " << cpp_strerror(r) << dendl;
    return r;
  }

  cls::rbd::MigrationSpec migration_spec = {cls::rbd::MIGRATION_TYPE_DST,
                                            cls::rbd::MIGRATION_STATE_STARTED,
                                            m_src_io_ctx.get_id(),
                                            m_src_image_name, m_src_image_id,
                                            snap_seqs, size, false};

  r = cls_client::migration_set(&m_dst_io_ctx, util::header_name(m_dst_image_id),
                                migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  {
    RWLock::RLocker owner_locker(dst_image_ctx->owner_lock);
    if (dst_image_ctx->exclusive_lock != nullptr) {
      dst_image_ctx->exclusive_lock->unblock_requests();
    }
  }
  dst_image_ctx->ignore_migrating = false;
  r = dst_image_ctx->state->refresh();
  if (r < 0) {
    lderr(m_cct) << "failed to refresh image state: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  *dst_image_ctx_ = dst_image_ctx;

  return 0;
}

template <typename I>
int Migrate<I>::disable_mirroring() {
  if (!m_src_image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    return 0;
  }

  ldout(m_cct, 10) << dendl;

  int r = cls_client::mirror_image_get(&m_src_io_ctx, m_src_image_ctx->id,
                                       &m_src_mirror_image);
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "mirroring is not enabled for this image" << dendl;
    return 0;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  C_SaferCond ctx;
  auto req = mirror::DisableRequest<I>::create(m_src_image_ctx, false, true, &ctx);
  req->send();
  r = ctx.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to disable mirroring: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  m_src_migration_spec.mirroring = true;

  return 0;
}

template <typename I>
int Migrate<I>::enable_mirroring() {
  if (!m_src_image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    return 0;
  }

  bool same_pool = m_src_io_ctx.get_id() == m_dst_io_ctx.get_id();

  if (same_pool &&
      m_src_mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    return 0;
  }

  ldout(m_cct, 10) << dendl;

  if (!same_pool) {
    cls::rbd::MirrorMode mirror_mode;
    int r = cls_client::mirror_mode_get(&m_dst_io_ctx, &mirror_mode);
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "failed to retrieve mirror mode: " << cpp_strerror(r)
                   << dendl;
      return r;
    }

    if (mirror_mode == cls::rbd::MIRROR_MODE_DISABLED) {
      ldout(m_cct, 10) << "mirroring is not enabled for destination pool"
                       << dendl;
      return 0;
    }
    if (mirror_mode == cls::rbd::MIRROR_MODE_IMAGE &&
        m_src_mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
      ldout(m_cct, 10) << "mirroring is not enabled for image" << dendl;
      return 0;
    }
  }

  C_SaferCond ctx;
  auto req = mirror::EnableRequest<I>::create(m_dst_io_ctx, m_dst_image_id, "",
                                              m_src_image_ctx->op_work_queue, &ctx);
  req->send();
  int r = ctx.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to enable mirroring: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migrate<I>::remove_src_image() {
  ldout(m_cct, 10) << dendl;

  m_src_migration_spec.state = cls::rbd::MIGRATION_STATE_COMPLETE;
  int r = cls_client::migration_set(&m_src_io_ctx, m_src_header_oid,
                                    m_src_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed updating migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  for (auto &snap : m_snaps) {
    librbd::NoOpProgressContext prog_ctx;
    int r = snap_remove(m_src_image_ctx, snap.name.c_str(), 0, prog_ctx);
    if (r < 0) {
      lderr(m_cct) << "failed removing snapshot '" << snap.name << "': "
                   << cpp_strerror(r) << dendl;
      return r;
    }
  }

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(m_cct, &thread_pool, &op_work_queue);
  librbd::NoOpProgressContext prog_ctx;
  C_SaferCond on_remove;
  auto req = librbd::image::RemoveRequest<I>::create(
      m_src_io_ctx, m_src_image_ctx, false, true, prog_ctx, op_work_queue,
      &on_remove);
  req->send();
  r = on_remove.wait();
  // For old format image it will return -ENOENT due to expected
  // tmap_rm failure at the end.
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed removing source image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (!m_src_image_id.empty()) {
    r = cls_client::trash_remove(&m_src_io_ctx, m_src_image_id);
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "error removing image " << m_src_image_id
                   << " from rbd_trash object" << dendl;
    }
  }

  return 0;
}

template <typename I>
int Migrate<I>::abort(const std::string &dst_image_id, bool mirroring) {
  ldout(m_cct, 10) << "dst_image_id=" << dst_image_id << ", mirroring="
                   << mirroring << dendl;

  m_dst_image_id = dst_image_id;
  int r;

  m_src_image_ctx->owner_lock.get_read();
  if (m_src_image_ctx->exclusive_lock != nullptr &&
      !m_src_image_ctx->exclusive_lock->is_lock_owner()) {
    C_SaferCond ctx;
    m_src_image_ctx->exclusive_lock->acquire_lock(&ctx);
    m_src_image_ctx->owner_lock.put_read();
    r = ctx.wait();
    if (r < 0) {
      lderr(m_cct) << "error acquiring exclusive lock: " << cpp_strerror(r)
                   << dendl;
      return r;
    }
  } else {
    m_src_image_ctx->owner_lock.put_read();
  }

  // XXXMG: purge snapshots

  ldout(m_cct, 10) << "removing dst image" << dendl;
  C_SaferCond on_remove;
  auto req = librbd::image::RemoveRequest<>::create(
    m_dst_io_ctx, m_dst_image_name, m_dst_image_id, false, false, m_prog_ctx,
    m_src_image_ctx->op_work_queue, &on_remove);
  req->send();
  r = on_remove.wait();
  if (r == -ENOENT) {
    ldout(m_cct, 0) << "destination image does not exit" << dendl;
  } else if (r < 0) {
    lderr(m_cct) << "failed removing destination image '"
                 << m_dst_io_ctx.get_pool_name() << "/" << m_dst_image_name
                 << " (" << m_dst_image_id << ")': " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  ldout(m_cct, 10) << "adding src image to directory" << dendl;
  if (m_src_old_format) {
    r = tmap_set(m_src_io_ctx, m_src_image_name);
    if (r < 0) {
      lderr(m_cct) << "failed adding " << m_src_image_name << " to tmap: "
                   << cpp_strerror(r) << dendl;
      return r;
    }
  } else {
    r = v2_relink_image();
    if (r < 0) {
      return r;
    }
  }

  r = unset_migration(m_src_image_ctx);
  if (r < 0) {
    return r;
  }

  if (mirroring) {
    ldout(m_cct, 10) << "re-enabling mirroring" << dendl;

    C_SaferCond ctx;
    auto req = mirror::EnableRequest<I>::create(m_src_io_ctx, m_src_image_id, "",
                                                m_src_image_ctx->op_work_queue, &ctx);
    req->send();
    r = ctx.wait();
    if (r < 0) {
      lderr(m_cct) << "failed to enable mirroring: " << cpp_strerror(r)
                   << dendl;
    }
    return r;
  }

  m_src_image_ctx->state->close();

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Migrate<librbd::ImageCtx>;
