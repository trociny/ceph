// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/internal.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"

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
struct ObjectCopyRequest<librbd::MockTestImageCtx> {
  static ObjectCopyRequest* s_instance;
  static ObjectCopyRequest* create(
      librbd::MockTestImageCtx *src_image_ctx,
      librbd::MockTestImageCtx *dst_image_ctx,
      const ImageCopyRequest<librbd::MockTestImageCtx>::SnapMap &snap_map,
      uint64_t object_number, Context *on_finish) {
    assert(s_instance != nullptr);
    Mutex::Locker locker(s_instance->lock);
    s_instance->snap_map = &snap_map;
    s_instance->object_contexts[object_number] = on_finish;
    s_instance->cond.Signal();
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  Mutex lock;
  Cond cond;

  const ImageCopyRequest<librbd::MockTestImageCtx>::SnapMap *snap_map = nullptr;
  std::map<uint64_t, Context *> object_contexts;

  ObjectCopyRequest() : lock("lock") {
    s_instance = this;
  }
};

ObjectCopyRequest<librbd::MockTestImageCtx>* ObjectCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy
} // namespace librbd

// template definitions
#include "librbd/deep_copy/ImageCopyRequest.cc"
template class librbd::deep_copy::ImageCopyRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;

class TestMockDeepCopyImageCopyRequest : public TestMockFixture {
public:
  typedef ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  ThreadPool *m_thread_pool;
  ContextWQ *m_work_queue;
  librbd::SnapSeqs m_snap_seqs;
  MockImageCopyRequest::SnapMap m_snap_map;
  librbd::NoOpProgressContext m_prog_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    librbd::RBD rbd;
    std::string dst_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(rbd, m_ioctx, dst_image_name, m_image_size));
    ASSERT_EQ(0, open_image(dst_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_thread_pool_instance(m_src_image_ctx->cct,
                                               &m_thread_pool, &m_work_queue);
  }

  void expect_get_image_size(librbd::MockTestImageCtx &mock_image_ctx,
                             uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_image_size(_))
      .WillOnce(Return(size)).RetiresOnSaturation();
  }

  void expect_object_copy_send(MockObjectCopyRequest &mock_object_copy_request) {
    EXPECT_CALL(mock_object_copy_request, send());
  }

  bool complete_object_copy(MockObjectCopyRequest &mock_object_copy_request,
                               uint64_t object_num, int r,
                               std::function<void()> fn = []() {}) {
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.object_contexts.count(object_num) == 0) {
      if (mock_object_copy_request.cond.WaitInterval(mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
        return false;
      }
    }

    FunctionContext *wrapper_ctx = new FunctionContext(
      [&mock_object_copy_request, object_num, fn] (int r) {
        fn();
        mock_object_copy_request.object_contexts[object_num]->complete(r);
      });
    m_work_queue->queue(wrapper_ctx, r);
    return true;
  }

  MockImageCopyRequest::SnapMap wait_for_snap_map(MockObjectCopyRequest &mock_object_copy_request) {
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.snap_map == nullptr) {
      if (mock_object_copy_request.cond.WaitInterval(mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
        return MockImageCopyRequest::SnapMap();
      }
    }
    return *mock_object_copy_request.snap_map;
  }

  MockImageCopyRequest *create_request(librbd::MockTestImageCtx &mock_src_image_ctx,
                                       librbd::MockTestImageCtx &mock_dst_image_ctx,
                                       const SnapSeqs &snap_seqs,
                                       Context *ctx) {
    return new MockImageCopyRequest(&mock_src_image_ctx,
                                    &mock_dst_image_ctx,
                                    snap_seqs, &m_prog_ctx, ctx);
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    int r = image_ctx->operations->snap_create(
        cls::rbd::UserSnapshotNamespace(), snap_name);
    if (r < 0) {
      return r;
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }

    if (image_ctx->snap_ids.count({cls::rbd::UserSnapshotNamespace(),
                                   snap_name}) == 0) {
      return -ENOENT;
    }

    if (snap_id != nullptr) {
      *snap_id = image_ctx->snap_ids[{cls::rbd::UserSnapshotNamespace(),
                                      snap_name}];
    }
    return 0;
  }

  int create_snap(const char* snap_name) {
    librados::snap_t src_snap_id;
    int r = create_snap(m_src_image_ctx, snap_name, &src_snap_id);
    if (r < 0) {
      return r;
    }

    librados::snap_t dst_snap_id;
    r = create_snap(m_dst_image_ctx, snap_name, &dst_snap_id);
    if (r < 0) {
      return r;
    }

    // collection of all existing snaps in dst image
    MockImageCopyRequest::SnapIds dst_snap_ids({dst_snap_id});
    if (!m_snap_map.empty()) {
      dst_snap_ids.insert(dst_snap_ids.end(),
                          m_snap_map.rbegin()->second.begin(),
                          m_snap_map.rbegin()->second.end());
    }
    m_snap_map[src_snap_id] = dst_snap_ids;
    m_snap_seqs[src_snap_id] = dst_snap_id;
    return 0;
  }
};

TEST_F(TestMockDeepCopyImageCopyRequest, SimpleImage) {
  ASSERT_EQ(0, create_snap("copy"));
  ASSERT_EQ(0, librbd::snap_set(m_src_image_ctx,
                                cls::rbd::UserSnapshotNamespace(), "copy"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  expect_get_image_size(mock_src_image_ctx, m_image_size);
  expect_get_image_size(mock_src_image_ctx, m_image_size);
  expect_object_copy_send(mock_object_copy_request);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_src_image_ctx,
                                                 mock_dst_image_ctx,
                                                 m_snap_seqs, &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, Snapshots) {
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2"));
  ASSERT_EQ(0, create_snap("copy"));
  ASSERT_EQ(0, librbd::snap_set(m_src_image_ctx,
                                cls::rbd::UserSnapshotNamespace(), "copy"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  expect_get_image_size(mock_src_image_ctx, m_image_size);
  expect_get_image_size(mock_src_image_ctx, m_image_size);
  expect_get_image_size(mock_src_image_ctx, m_image_size);
  expect_get_image_size(mock_src_image_ctx, m_image_size);
  expect_object_copy_send(mock_object_copy_request);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_src_image_ctx,
                                                 mock_dst_image_ctx,
                                                 m_snap_seqs, &ctx);
  request->send();

  MockImageCopyRequest::SnapMap snap_map(m_snap_map);
  ASSERT_EQ(snap_map, wait_for_snap_map(mock_object_copy_request));

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_EQ(0, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
