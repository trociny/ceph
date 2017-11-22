// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_MIGRATE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_MIGRATE_REQUEST_H

#include "librbd/operation/Request.h"
#include "common/snap_types.h"
#include "librbd/Types.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class MigrateRequest : public Request<ImageCtxT>
{
public:
  MigrateRequest(ImageCtxT &image_ctx, Context *on_finish,
                 uint64_t overlap_objects, const ::SnapContext &snapc,
                 ProgressContext &prog_ctx)
    : Request<ImageCtxT>(image_ctx, on_finish),
    m_overlap_objects(overlap_objects), m_snapc(snapc), m_prog_ctx(prog_ctx) {
  }

protected:
  void send_op() override;
  bool should_complete(int r) override;
  bool can_affect_io() const override {
    return true;
  }
  journal::Event create_event(uint64_t op_tid) const override {
    assert(0);
    return journal::UnknownEvent();
  }

private:
  /**
   * Migrate goes through the following state machine to copyup objects
   * from the parent (migrating source) image:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_MIGRATE_OBJECTS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   *
   */

  uint64_t m_object_size;
  uint64_t m_overlap_objects;
  ::SnapContext m_snapc;
  ProgressContext &m_prog_ctx;
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::MigrateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_MIGRATE_REQUEST_H
