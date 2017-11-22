// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/MigrateRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/ObjectRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MigrateRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace operation {

template <typename I>
class C_MigrateObject : public C_AsyncObjectThrottle<I> {
public:
  C_MigrateObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                  ::SnapContext snapc, uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_snapc(snapc),
      m_object_no(object_no) {
  }

  int send() override {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.owner_lock.is_locked());
    CephContext *cct = image_ctx.cct;

    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during migrate" << dendl;
      return -ERESTART;
    }

    bufferlist bl;
    string oid = image_ctx.get_object_name(m_object_no);
    auto req = new io::ObjectWriteRequest<I>(&image_ctx, oid, m_object_no, 0,
                                             bl, m_snapc, 0, {}, this);
    if (!req->has_parent()) {
      delete req;
      return -ESTALE;
    }

    req->send();
    return 0;
  }

private:
  uint64_t m_object_size;
  ::SnapContext m_snapc;
  uint64_t m_object_no;
};

template <typename I>
bool MigrateRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void MigrateRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_MigrateObject<I> >(),
      boost::lambda::_1, &image_ctx, m_snapc, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, this->create_callback_context(), &m_prog_ctx,
    0, m_overlap_objects);
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::MigrateRequest<librbd::ImageCtx>;
