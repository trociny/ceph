// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/Threads.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  Mutex &timer_lock;
  SafeTimer *timer;
  ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

template<>
struct LeaderWatcher<librbd::MockTestImageCtx> {
  MOCK_METHOD0(get_instance_id, std::string());

  MOCK_METHOD2(notify_sync_request, void(const std::string &,
                                         const std::string &));
  MOCK_METHOD2(notify_sync_request_ack, void(const std::string &,
                                             const std::string &));
  MOCK_METHOD2(notify_sync_start, void(const std::string &,
                                       const std::string &));
  MOCK_METHOD2(notify_sync_complete, void(const std::string &,
                                          const std::string &));
};

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/InstanceSyncThrottler.cc"

namespace rbd {
namespace mirror {

using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

class TestMockInstanceSyncThrottler : public TestMockFixture {
public:
  typedef InstanceSyncThrottler<librbd::MockTestImageCtx> MockInstanceSyncThrottler;
  typedef LeaderWatcher<librbd::MockTestImageCtx> MockLeaderWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  MockLeaderWatcher *m_mock_leader_watcher;
  MockThreads *m_mock_threads;

  void SetUp() override {
    TestMockFixture::SetUp();

    m_mock_threads = new MockThreads(m_threads);
    m_mock_leader_watcher = new MockLeaderWatcher();
    EXPECT_CALL(*m_mock_leader_watcher, get_instance_id())
      .WillOnce(Return("instance_id"));
  }

  void TearDown() override {
    delete m_mock_leader_watcher;
    delete m_mock_threads;

    TestMockFixture::TearDown();
  }

  void expect_notify_sync_request(const std::string &request_id) {
    EXPECT_CALL(*m_mock_leader_watcher, notify_sync_request("instance_id",
                                                            request_id));
  }

  void expect_notify_sync_request(const std::string &request_id,
                                  Context *on_sync_request) {
    EXPECT_CALL(*m_mock_leader_watcher, notify_sync_request("instance_id",
                                                            request_id))
        .WillOnce(Invoke([on_sync_request](const std::string &instance_id,
                                           const std::string &request_id) {
                           on_sync_request->complete(0);
                         }));
  }

  void expect_notify_sync_request(const std::string &request_id,
                                  MockInstanceSyncThrottler &throttler) {
    EXPECT_CALL(*m_mock_leader_watcher, notify_sync_request("instance_id",
                                                            request_id))
        .WillOnce(Invoke([this, &throttler](const std::string &instance_id,
                                            const std::string &request_id) {
                           auto ctx = new FunctionContext(
                               [&throttler, instance_id, request_id](int r) {
                                 throttler.handle_sync_request_ack(instance_id,
                                                                   request_id);
                               });
                           m_mock_threads->work_queue->queue(ctx, 0);
                         }));
    EXPECT_CALL(*m_mock_leader_watcher, notify_sync_start("instance_id",
                                                          request_id));
  }

  void expect_notify_sync_start(const std::string &request_id) {
    EXPECT_CALL(*m_mock_leader_watcher, notify_sync_start("instance_id",
                                                          request_id));
  }

  void expect_notify_sync_complete(const std::string &request_id) {
    EXPECT_CALL(*m_mock_leader_watcher, notify_sync_complete("instance_id",
                                                             request_id));
  }
};

TEST_F(TestMockInstanceSyncThrottler, Single_Sync) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.handle_leader_acquired();

  C_SaferCond on_start;
  throttler.start_op("id", &on_start);
  ASSERT_EQ(0, on_start.wait());
  throttler.finish_op("id");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Single_Sync_Proxy) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);

  InSequence seq;

  expect_notify_sync_request("id", throttler);
  C_SaferCond on_start;
  throttler.start_op("id", &on_start);
  ASSERT_EQ(0, on_start.wait());

  expect_notify_sync_complete("id");
  throttler.finish_op("id");
}

TEST_F(TestMockInstanceSyncThrottler, Multiple_Syncs) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.set_max_concurrent_syncs(2);
  throttler.handle_leader_acquired();

  C_SaferCond on_start1;
  throttler.start_op("id1", &on_start1);
  C_SaferCond on_start2;
  throttler.start_op("id2", &on_start2);
  C_SaferCond on_start3;
  throttler.start_op("id3", &on_start3);
  C_SaferCond on_start4;
  throttler.start_op("id4", &on_start4);

  ASSERT_EQ(0, on_start2.wait());
  throttler.finish_op("id2");
  ASSERT_EQ(0, on_start3.wait());
  throttler.finish_op("id3");
  ASSERT_EQ(0, on_start1.wait());
  throttler.finish_op("id1");
  ASSERT_EQ(0, on_start4.wait());
  throttler.finish_op("id4");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Multiple_Syncs_Proxy) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);

  InSequence seq;

  expect_notify_sync_request("id1", throttler);
  C_SaferCond on_start1;
  throttler.start_op("id1", &on_start1);
  ASSERT_EQ(0, on_start1.wait());

  expect_notify_sync_request("id2", throttler);
  C_SaferCond on_start2;
  throttler.start_op("id2", &on_start2);
  ASSERT_EQ(0, on_start2.wait());

  expect_notify_sync_request("id3");
  C_SaferCond on_start3;
  throttler.start_op("id3", &on_start3);

  expect_notify_sync_request("id4");
  C_SaferCond on_start4;
  throttler.start_op("id4", &on_start4);

  expect_notify_sync_complete("id2");
  throttler.finish_op("id2");

  expect_notify_sync_start("id3");
  throttler.handle_sync_request_ack("instance_id", "id3");
  ASSERT_EQ(0, on_start3.wait());
  expect_notify_sync_complete("id3");
  throttler.finish_op("id3");

  expect_notify_sync_complete("id1");
  throttler.finish_op("id1");

  expect_notify_sync_start("id4");
  throttler.handle_sync_request_ack("instance_id", "id4");
  ASSERT_EQ(0, on_start4.wait());
  expect_notify_sync_complete("id4");
  throttler.finish_op("id4");
}

TEST_F(TestMockInstanceSyncThrottler, Cancel_Running_Sync) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.handle_leader_acquired();

  C_SaferCond on_start;
  throttler.start_op("id", &on_start);
  ASSERT_EQ(0, on_start.wait());
  ASSERT_FALSE(throttler.cancel_op("id"));
  throttler.finish_op("id");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Cancel_Running_Sync_Proxy) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);

  InSequence seq;

  expect_notify_sync_request("id", throttler);
  C_SaferCond on_start;
  throttler.start_op("id", &on_start);
  ASSERT_EQ(0, on_start.wait());

  ASSERT_FALSE(throttler.cancel_op("id"));
  expect_notify_sync_complete("id");
  throttler.finish_op("id");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Cancel_Waiting_Sync) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.set_max_concurrent_syncs(1);
  throttler.handle_leader_acquired();

  C_SaferCond on_start1;
  throttler.start_op("id1", &on_start1);
  C_SaferCond on_start2;
  throttler.start_op("id2", &on_start2);

  ASSERT_EQ(0, on_start1.wait());
  ASSERT_TRUE(throttler.cancel_op("id2"));
  ASSERT_EQ(-ECANCELED, on_start2.wait());
  throttler.finish_op("id1");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Cancel_Waiting_Sync_Proxy) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);

  InSequence seq;

  C_SaferCond on_sync_request;
  expect_notify_sync_request("id", &on_sync_request);
  C_SaferCond on_start;
  throttler.start_op("id", &on_start);

  ASSERT_EQ(0, on_sync_request.wait());
  expect_notify_sync_complete("id");
  ASSERT_TRUE(throttler.cancel_op("id"));
  ASSERT_EQ(-ECANCELED, on_start.wait());
}

TEST_F(TestMockInstanceSyncThrottler, Cancel_Running_Sync_Start_Waiting) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.set_max_concurrent_syncs(1);
  throttler.handle_leader_acquired();

  C_SaferCond on_start1;
  throttler.start_op("id1", &on_start1);
  C_SaferCond on_start2;
  throttler.start_op("id2", &on_start2);

  ASSERT_EQ(0, on_start1.wait());
  ASSERT_FALSE(throttler.cancel_op("id1"));
  throttler.finish_op("id1");
  ASSERT_EQ(0, on_start2.wait());
  throttler.finish_op("id2");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Increase_Max_Concurrent_Syncs) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.set_max_concurrent_syncs(2);
  throttler.handle_leader_acquired();

  C_SaferCond on_start1;
  throttler.start_op("id1", &on_start1);
  C_SaferCond on_start2;
  throttler.start_op("id2", &on_start2);
  C_SaferCond on_start3;
  throttler.start_op("id3", &on_start3);
  C_SaferCond on_start4;
  throttler.start_op("id4", &on_start4);
  C_SaferCond on_start5;
  throttler.start_op("id5", &on_start5);

  ASSERT_EQ(0, on_start1.wait());
  ASSERT_EQ(0, on_start2.wait());

  throttler.set_max_concurrent_syncs(4);

  ASSERT_EQ(0, on_start3.wait());
  ASSERT_EQ(0, on_start4.wait());

  throttler.finish_op("id4");
  ASSERT_EQ(0, on_start5.wait());

  throttler.finish_op("id1");
  throttler.finish_op("id2");
  throttler.finish_op("id3");
  throttler.finish_op("id5");

  throttler.handle_leader_released();
}

TEST_F(TestMockInstanceSyncThrottler, Decrease_Max_Concurrent_Syncs) {
  MockInstanceSyncThrottler throttler(m_mock_threads, m_mock_leader_watcher);
  throttler.set_max_concurrent_syncs(4);
  throttler.handle_leader_acquired();

  C_SaferCond on_start1;
  throttler.start_op("id1", &on_start1);
  C_SaferCond on_start2;
  throttler.start_op("id2", &on_start2);
  C_SaferCond on_start3;
  throttler.start_op("id3", &on_start3);
  C_SaferCond on_start4;
  throttler.start_op("id4", &on_start4);
  C_SaferCond on_start5;
  throttler.start_op("id5", &on_start5);

  ASSERT_EQ(0, on_start1.wait());
  ASSERT_EQ(0, on_start2.wait());
  ASSERT_EQ(0, on_start3.wait());
  ASSERT_EQ(0, on_start4.wait());

  throttler.set_max_concurrent_syncs(2);

  throttler.finish_op("id1");
  throttler.finish_op("id2");
  throttler.finish_op("id3");

  ASSERT_EQ(0, on_start5.wait());

  throttler.finish_op("id4");
  throttler.finish_op("id5");

  throttler.handle_leader_released();
}

} // namespace mirror
} // namespace rbd

