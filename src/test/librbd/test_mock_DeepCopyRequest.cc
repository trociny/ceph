// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/internal.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockObjectMap.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace deep_copy {

template <>
class ImageCopyRequest<librbd::MockTestImageCtx> {
public:
  static ImageCopyRequest* s_instance;
  Context *on_finish;

  static ImageCopyRequest* create(
      librbd::MockTestImageCtx *src_image_ctx,
      librbd::MockTestImageCtx *dst_image_ctx, const SnapSeqs &snap_seqs,
      ProgressContext *prog_ctx, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  ImageCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());
};

template <>
class SnapshotCopyRequest<librbd::MockTestImageCtx> {
public:
  static SnapshotCopyRequest* s_instance;
  Context *on_finish;

  static SnapshotCopyRequest* create(librbd::MockTestImageCtx *src_image_ctx,
                                     librbd::MockTestImageCtx *dst_image_ctx,
                                     ContextWQ *work_queue, SnapSeqs *snap_seqs,
                                     Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SnapshotCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());
};

ImageCopyRequest<librbd::MockTestImageCtx>* ImageCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
SnapshotCopyRequest<librbd::MockTestImageCtx>* SnapshotCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy

namespace operation {

template <>
class SnapshotRemoveRequest<librbd::MockTestImageCtx> {
public:
  static SnapshotRemoveRequest* s_instance;
  Context *on_finish;

  static SnapshotRemoveRequest* create(
      librbd::MockTestImageCtx &image_ctx,
      const cls::rbd::SnapshotNamespace &snap_namespace,
      const std::string &snap_name, uint64_t snap_id, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SnapshotRemoveRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(send, void());
};

SnapshotRemoveRequest<librbd::MockTestImageCtx>* SnapshotRemoveRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace operation
} // namespace librbd

// template definitions
template class librbd::DeepCopyRequest<librbd::MockTestImageCtx>;
#include "librbd/DeepCopyRequest.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnNew;
using ::testing::WithArg;

class TestMockDeepCopyRequest : public TestMockFixture {
public:
  typedef librbd::DeepCopyRequest<librbd::MockTestImageCtx> MockDeepCopyRequest;
  typedef librbd::deep_copy::ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef librbd::deep_copy::SnapshotCopyRequest<librbd::MockTestImageCtx> MockSnapshotCopyRequest;
  typedef librbd::operation::SnapshotRemoveRequest<librbd::MockTestImageCtx> MockSnapshotRemoveRequest;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  ThreadPool *m_thread_pool;
  ContextWQ *m_work_queue;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    EXPECT_EQ(0, snap_create(*m_src_image_ctx, "copy"));
    EXPECT_EQ(0, librbd::snap_set(m_src_image_ctx,
                                  cls::rbd::UserSnapshotNamespace(), "copy"));

    ASSERT_EQ(0, open_image(m_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_thread_pool_instance(m_src_image_ctx->cct,
                                               &m_thread_pool, &m_work_queue);
  }

  void TearDown() override {
    EXPECT_EQ(0, m_src_image_ctx->operations->snap_remove(
                cls::rbd::UserSnapshotNamespace(), "copy"));

    TestMockFixture::TearDown();
  }

  void expect_test_features(librbd::MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_, _))
      .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
              return (mock_image_ctx.features & features) != 0;
            })));
  }

  void expect_start_op(librbd::MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(mock_exclusive_lock, start_op()).WillOnce(
      ReturnNew<FunctionContext>([](int) {}));
  }

  void expect_rollback_object_map(librbd::MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, rollback(_, _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *ctx) {
              m_work_queue->queue(ctx, r);
            })));
  }

  void expect_create_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                                librbd::MockObjectMap *mock_object_map) {
    EXPECT_CALL(mock_image_ctx, create_object_map(CEPH_NOSNAP))
      .WillOnce(Return(mock_object_map));
  }

  void expect_open_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                              librbd::MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, open(_))
      .WillOnce(Invoke([this](Context *ctx) {
          m_work_queue->queue(ctx, 0);
        }));
  }

  void expect_copy_snapshots(
      MockSnapshotCopyRequest &mock_snapshot_copy_request, int r) {
    EXPECT_CALL(mock_snapshot_copy_request, send())
      .WillOnce(Invoke([this, &mock_snapshot_copy_request, r]() {
            m_work_queue->queue(mock_snapshot_copy_request.on_finish, r);
          }));
  }

  void expect_copy_image(MockImageCopyRequest &mock_image_copy_request, int r) {
    EXPECT_CALL(mock_image_copy_request, send())
      .WillOnce(Invoke([this, &mock_image_copy_request, r]() {
            m_work_queue->queue(mock_image_copy_request.on_finish, r);
          }));
  }

  void expect_copy_object_map(librbd::MockExclusiveLock &mock_exclusive_lock,
                              librbd::MockObjectMap *mock_object_map) {
    expect_start_op(mock_exclusive_lock);
    expect_rollback_object_map(*mock_object_map, 0);
  }

  void expect_refresh_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                                 librbd::MockObjectMap *mock_object_map) {
    expect_create_object_map(mock_image_ctx, mock_object_map);
    expect_open_object_map(mock_image_ctx, *mock_object_map);
  }

  void expect_remove_copy_snapshot(
      MockSnapshotRemoveRequest &mock_snapshot_remove_request, int r) {
    EXPECT_CALL(mock_snapshot_remove_request, send())
      .WillOnce(Invoke([this, &mock_snapshot_remove_request, r]() {
            m_work_queue->queue(mock_snapshot_remove_request.on_finish, r);
          }));
  }
};

TEST_F(TestMockDeepCopyRequest, SimpleCopy) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSnapshotRemoveRequest mock_snapshot_remove_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_dst_image_ctx.object_map = mock_object_map;
  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  if ((m_dst_image_ctx->features & RBD_FEATURE_OBJECT_MAP) != 0) {
    expect_copy_object_map(mock_exclusive_lock, mock_object_map);
    expect_refresh_object_map(mock_dst_image_ctx, mock_object_map);
  }
  expect_remove_copy_snapshot(mock_snapshot_remove_request, 0);

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, m_work_queue, &no_op, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnCopySnapshots) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, -EINVAL);

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, m_work_queue, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnCopyImage) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, -EINVAL);

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, m_work_queue, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnRemoveCopySnapshot) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSnapshotRemoveRequest mock_snapshot_remove_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_dst_image_ctx.object_map = mock_object_map;
  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  if ((m_dst_image_ctx->features & RBD_FEATURE_OBJECT_MAP) != 0) {
    expect_copy_object_map(mock_exclusive_lock, mock_object_map);
    expect_refresh_object_map(mock_dst_image_ctx, mock_object_map);
  }
  expect_remove_copy_snapshot(mock_snapshot_remove_request, -EINVAL);

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, m_work_queue, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}
