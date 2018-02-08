// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "librbd/image/ListWatchersRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/image/ListWatchersRequest.cc"
template class librbd::image::ListWatchersRequest<librbd::MockImageCtx>;

namespace librbd {

namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;

class TestMockListWatchersRequest : public TestMockFixture {
public:
  typedef ListWatchersRequest<MockImageCtx> MockListWatchersRequest;

  obj_watch_t watcher(const std::string &address, uint64_t watch_handle) {
    obj_watch_t w;
    strcpy(w.addr, address.c_str());
    w.watcher_id = 0;
    w.cookie = watch_handle;
    w.timeout_seconds = 0;

    return w;
  }

  void expect_list_watchers(MockTestImageCtx &mock_image_ctx,
                            const std::string oid,
                            const std::list<obj_watch_t> &watchers, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               list_watchers(oid, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoAll(SetArgPointee<1>(watchers), Return(0)));
    }
  }

  void expect_list_image_watchers(MockTestImageCtx &mock_image_ctx,
                                  const std::list<obj_watch_t> &watchers,
                                  int r) {
    expect_list_watchers(mock_image_ctx, mock_image_ctx.header_oid,
                         watchers, r);
  }

  void expect_list_mirror_watchers(MockTestImageCtx &mock_image_ctx,
                                   const std::list<obj_watch_t> &watchers,
                                   int r) {
    expect_list_watchers(mock_image_ctx, RBD_MIRRORING, watchers, r);
  }

  void expect_mirror_image_get(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get"),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_get_watch_handle(MockImageWatcher &mock_watcher,
                               uint64_t watch_handle) {
    EXPECT_CALL(mock_watcher, get_watch_handle())
      .WillOnce(Return(watch_handle));
  }
};

TEST_F(TestMockListWatchersRequest, NoImageWatchers) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageWatcher mock_watcher;

  InSequence seq;
  expect_list_image_watchers(mock_image_ctx, {}, 0);

  std::list<WatcherInfo> watchers;
  C_SaferCond ctx;
  auto req = MockListWatchersRequest::create(mock_image_ctx, &watchers, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(watchers.empty());
}

TEST_F(TestMockListWatchersRequest, Error) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageWatcher mock_watcher;

  InSequence seq;
  expect_list_image_watchers(mock_image_ctx, {}, -EINVAL);

  std::list<WatcherInfo> watchers;
  C_SaferCond ctx;
  auto req = MockListWatchersRequest::create(mock_image_ctx, &watchers, &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockListWatchersRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageWatcher mock_watcher;

  InSequence seq;
  expect_list_image_watchers(mock_image_ctx,
                             {watcher("a", 123), watcher("b", 456)}, 0);
  if (is_feature_enabled(RBD_FEATURE_JOURNALING)) {
    expect_mirror_image_get(mock_image_ctx, 0);
    expect_list_mirror_watchers(mock_image_ctx, {watcher("b", 789)}, 0);
  }
  expect_get_watch_handle(*mock_image_ctx.image_watcher, 123);

  std::list<WatcherInfo> watchers;
  C_SaferCond ctx;
  auto req = MockListWatchersRequest::create(mock_image_ctx, &watchers, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(2U, watchers.size());

  auto w = watchers.begin();
  ASSERT_EQ("a", w->addr);
  ASSERT_EQ(123, w->cookie);
  ASSERT_FALSE(w->mirroring);
  ASSERT_TRUE(w->me);

  w++;
  ASSERT_EQ("b", w->addr);
  ASSERT_EQ(456, w->cookie);
  if (is_feature_enabled(RBD_FEATURE_JOURNALING)) {
    ASSERT_TRUE(w->mirroring);
  } else {
    ASSERT_FALSE(w->mirroring);
  }
  ASSERT_FALSE(w->me);
}

} // namespace image
} // namespace librbd
