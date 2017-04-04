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

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/TypeTraits.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/ImageState.h"
#include "tools/rbd_mirror/ImageSync.h"
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

namespace journal {

template <>
struct TypeTraits<librbd::MockTestImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

namespace rbd {
namespace mirror {

using ::testing::Invoke;
using ::testing::Return;

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

typedef ImageSync<librbd::MockTestImageCtx> MockImageSync;

template<>
class ImageSync<librbd::MockTestImageCtx> {
public:
  static std::vector<MockImageSync *> instances;

  Context *on_finish;
  C_SaferCond on_sync_start;
  bool syncing = false;

  static ImageSync* create(librbd::MockTestImageCtx *local_image_ctx,
                           librbd::MockTestImageCtx *remote_image_ctx,
                           SafeTimer *timer, Mutex *timer_lock,
                           const std::string &mirror_uuid,
                           journal::MockJournaler *journaler,
                           librbd::journal::MirrorPeerClientMeta *client_meta,
                           ContextWQ *work_queue, Context *on_finish,
                           ProgressContext *progress_ctx = nullptr) {
    ImageSync *sync = new ImageSync();
    sync->on_finish = on_finish;

    EXPECT_CALL(*sync, send())
      .WillRepeatedly(Invoke([sync]() {
            sync->syncing = true;
            sync->on_sync_start.complete(0);
          }));

    return sync;
  }

  void finish(int r) {
    on_finish->complete(r);
    put();
  }

  void get() {
    instances.push_back(this);
  }

  void put() { delete this; }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());

};


std::vector<MockImageSync *> MockImageSync::instances;

} // namespace mirror
} // namespace rbd


// template definitions
#include "tools/rbd_mirror/InstanceSyncThrottler.cc"
#include "tools/rbd_mirror/ImageSyncThrottler.cc"

namespace rbd {
namespace mirror {

class TestMockImageSyncThrottler : public TestMockFixture {
public:
  typedef ImageSyncThrottler<librbd::MockTestImageCtx> MockImageSyncThrottler;
  typedef InstanceSyncThrottler<librbd::MockTestImageCtx> MockInstanceSyncThrottler;
  typedef LeaderWatcher<librbd::MockTestImageCtx> MockLeaderWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  void SetUp() override {
    TestMockFixture::SetUp();

    m_mock_threads = new MockThreads(m_threads);

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));

    m_mock_leader_watcher = new MockLeaderWatcher();
    EXPECT_CALL(*m_mock_leader_watcher, get_instance_id())
        .WillOnce(Return("instance_id"));
    m_mock_instance_sync_throttler = new MockInstanceSyncThrottler(
        m_mock_threads, m_mock_leader_watcher);
    m_mock_instance_sync_throttler->handle_leader_acquired();
    mock_sync_throttler =
        new MockImageSyncThrottler(m_mock_instance_sync_throttler);

    m_mock_local_image_ctx = new librbd::MockTestImageCtx(*m_local_image_ctx);
    m_mock_remote_image_ctx = new librbd::MockTestImageCtx(*m_remote_image_ctx);
    m_mock_journaler = new journal::MockJournaler();
  }

  void TearDown() override {
    m_mock_instance_sync_throttler->handle_leader_released();
    MockImageSync::instances.clear();
    delete mock_sync_throttler;
    delete m_mock_instance_sync_throttler;
    delete m_mock_leader_watcher;
    delete m_mock_local_image_ctx;
    delete m_mock_remote_image_ctx;
    delete m_mock_journaler;
    delete m_mock_threads;
    TestMockFixture::TearDown();
  }

  void start_sync(const std::string& image_id, Context *ctx) {
    m_mock_local_image_ctx->id = image_id;
    mock_sync_throttler->start_sync(m_mock_local_image_ctx,
                                    m_mock_remote_image_ctx,
                                    m_threads->timer,
                                    &m_threads->timer_lock,
                                    "mirror_uuid",
                                    m_mock_journaler,
                                    &m_client_meta,
                                    m_threads->work_queue,
                                    ctx);
  }

  void cancel(const std::string& mirror_uuid, MockImageSync *sync,
              bool running=true) {
    if (running) {
      EXPECT_CALL(*sync, cancel())
        .WillOnce(Invoke([sync]() {
              sync->finish(-ECANCELED);
            }));
    } else {
      EXPECT_CALL(*sync, cancel()).Times(0);
    }
    mock_sync_throttler->cancel_sync(mirror_uuid);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::MockTestImageCtx *m_mock_local_image_ctx;
  librbd::MockTestImageCtx *m_mock_remote_image_ctx;
  journal::MockJournaler *m_mock_journaler;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
  MockLeaderWatcher *m_mock_leader_watcher;
  MockInstanceSyncThrottler *m_mock_instance_sync_throttler;
  MockImageSyncThrottler *mock_sync_throttler;
  MockThreads *m_mock_threads;
};

TEST_F(TestMockImageSyncThrottler, Single_Sync) {
  C_SaferCond ctx;
  start_sync("image_id", &ctx);

  ASSERT_EQ(1u, MockImageSync::instances.size());
  MockImageSync *sync = MockImageSync::instances[0];
  ASSERT_EQ(0, sync->on_sync_start.wait());
  ASSERT_EQ(true, sync->syncing);
  sync->finish(0);
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncThrottler, Multiple_Syncs) {
  m_mock_instance_sync_throttler->set_max_concurrent_syncs(2);

  C_SaferCond ctx1;
  start_sync("image_id_1", &ctx1);
  C_SaferCond ctx2;
  start_sync("image_id_2", &ctx2);
  C_SaferCond ctx3;
  start_sync("image_id_3", &ctx3);
  C_SaferCond ctx4;
  start_sync("image_id_4", &ctx4);

  ASSERT_EQ(4u, MockImageSync::instances.size());

  MockImageSync *sync1 = MockImageSync::instances[0];
  ASSERT_EQ(0, sync1->on_sync_start.wait());
  ASSERT_TRUE(sync1->syncing);

  MockImageSync *sync2 = MockImageSync::instances[1];
  ASSERT_EQ(0, sync2->on_sync_start.wait());
  ASSERT_TRUE(sync2->syncing);

  MockImageSync *sync3 = MockImageSync::instances[2];
  ASSERT_FALSE(sync3->syncing);

  MockImageSync *sync4 = MockImageSync::instances[3];
  ASSERT_FALSE(sync4->syncing);

  sync1->finish(0);
  ASSERT_EQ(0, ctx1.wait());

  ASSERT_EQ(0, sync3->on_sync_start.wait());
  ASSERT_TRUE(sync3->syncing);
  sync3->finish(-EINVAL);
  ASSERT_EQ(-EINVAL, ctx3.wait());

  ASSERT_EQ(0, sync4->on_sync_start.wait());
  ASSERT_TRUE(sync4->syncing);

  sync2->finish(0);
  ASSERT_EQ(0, ctx2.wait());

  sync4->finish(0);
  ASSERT_EQ(0, ctx4.wait());
}

TEST_F(TestMockImageSyncThrottler, Cancel_Running_Sync) {
  C_SaferCond ctx1;
  start_sync("image_id_1", &ctx1);
  C_SaferCond ctx2;
  start_sync("image_id_2", &ctx2);

  ASSERT_EQ(2u, MockImageSync::instances.size());

  MockImageSync *sync1 = MockImageSync::instances[0];
  ASSERT_EQ(0, sync1->on_sync_start.wait());
  ASSERT_TRUE(sync1->syncing);

  MockImageSync *sync2 = MockImageSync::instances[1];
  ASSERT_EQ(0, sync2->on_sync_start.wait());
  ASSERT_TRUE(sync2->syncing);

  cancel("image_id_2", sync2);
  ASSERT_EQ(-ECANCELED, ctx2.wait());

  sync1->finish(0);
  ASSERT_EQ(0, ctx1.wait());
}

TEST_F(TestMockImageSyncThrottler, Cancel_Waiting_Sync) {
  m_mock_instance_sync_throttler->set_max_concurrent_syncs(1);

  C_SaferCond ctx1;
  start_sync("image_id_1", &ctx1);
  C_SaferCond ctx2;
  start_sync("image_id_2", &ctx2);

  ASSERT_EQ(2u, MockImageSync::instances.size());

  MockImageSync *sync1 = MockImageSync::instances[0];
  ASSERT_EQ(0, sync1->on_sync_start.wait());
  ASSERT_TRUE(sync1->syncing);

  MockImageSync *sync2 = MockImageSync::instances[1];
  ASSERT_FALSE(sync2->syncing);

  cancel("image_id_2", sync2, false);
  ASSERT_EQ(-ECANCELED, ctx2.wait());

  sync1->finish(0);
  ASSERT_EQ(0, ctx1.wait());
}

TEST_F(TestMockImageSyncThrottler, Cancel_Running_Sync_Start_Waiting) {
  m_mock_instance_sync_throttler->set_max_concurrent_syncs(1);

  C_SaferCond ctx1;
  start_sync("image_id_1", &ctx1);
  C_SaferCond ctx2;
  start_sync("image_id_2", &ctx2);

  ASSERT_EQ(2u, MockImageSync::instances.size());

  MockImageSync *sync1 = MockImageSync::instances[0];
  ASSERT_EQ(0, sync1->on_sync_start.wait());
  ASSERT_TRUE(sync1->syncing);

  MockImageSync *sync2 = MockImageSync::instances[1];
  ASSERT_FALSE(sync2->syncing);

  cancel("image_id_1", sync1);
  ASSERT_EQ(-ECANCELED, ctx1.wait());

  ASSERT_EQ(0, sync2->on_sync_start.wait());
  ASSERT_TRUE(sync2->syncing);
  sync2->finish(0);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockImageSyncThrottler, Increase_Max_Concurrent_Syncs) {
  m_mock_instance_sync_throttler->set_max_concurrent_syncs(2);

  C_SaferCond ctx1;
  start_sync("image_id_1", &ctx1);
  C_SaferCond ctx2;
  start_sync("image_id_2", &ctx2);
  C_SaferCond ctx3;
  start_sync("image_id_3", &ctx3);
  C_SaferCond ctx4;
  start_sync("image_id_4", &ctx4);
  C_SaferCond ctx5;
  start_sync("image_id_5", &ctx5);

  ASSERT_EQ(5u, MockImageSync::instances.size());

  MockImageSync *sync1 = MockImageSync::instances[0];
  ASSERT_EQ(0, sync1->on_sync_start.wait());
  ASSERT_TRUE(sync1->syncing);

  MockImageSync *sync2 = MockImageSync::instances[1];
  ASSERT_EQ(0, sync2->on_sync_start.wait());
  ASSERT_TRUE(sync2->syncing);

  MockImageSync *sync3 = MockImageSync::instances[2];
  ASSERT_FALSE(sync3->syncing);

  MockImageSync *sync4 = MockImageSync::instances[3];
  ASSERT_FALSE(sync4->syncing);

  MockImageSync *sync5 = MockImageSync::instances[4];
  ASSERT_FALSE(sync5->syncing);

  m_mock_instance_sync_throttler->set_max_concurrent_syncs(4);

  ASSERT_EQ(0, sync3->on_sync_start.wait());
  ASSERT_TRUE(sync3->syncing);
  ASSERT_EQ(0, sync4->on_sync_start.wait());
  ASSERT_TRUE(sync4->syncing);
  ASSERT_FALSE(sync5->syncing);

  sync1->finish(0);
  ASSERT_EQ(0, ctx1.wait());

  ASSERT_EQ(0, sync5->on_sync_start.wait());
  ASSERT_TRUE(sync5->syncing);
  sync5->finish(-EINVAL);
  ASSERT_EQ(-EINVAL, ctx5.wait());

  sync2->finish(0);
  ASSERT_EQ(0, ctx2.wait());

  sync3->finish(0);
  ASSERT_EQ(0, ctx3.wait());

  sync4->finish(0);
  ASSERT_EQ(0, ctx4.wait());
}

TEST_F(TestMockImageSyncThrottler, Decrease_Max_Concurrent_Syncs) {
  m_mock_instance_sync_throttler->set_max_concurrent_syncs(4);

  C_SaferCond ctx1;
  start_sync("image_id_1", &ctx1);
  C_SaferCond ctx2;
  start_sync("image_id_2", &ctx2);
  C_SaferCond ctx3;
  start_sync("image_id_3", &ctx3);
  C_SaferCond ctx4;
  start_sync("image_id_4", &ctx4);
  C_SaferCond ctx5;
  start_sync("image_id_5", &ctx5);

  ASSERT_EQ(5u, MockImageSync::instances.size());

  MockImageSync *sync1 = MockImageSync::instances[0];
  ASSERT_EQ(0, sync1->on_sync_start.wait());
  ASSERT_TRUE(sync1->syncing);

  MockImageSync *sync2 = MockImageSync::instances[1];
  ASSERT_EQ(0, sync2->on_sync_start.wait());
  ASSERT_TRUE(sync2->syncing);

  MockImageSync *sync3 = MockImageSync::instances[2];
  ASSERT_EQ(0, sync3->on_sync_start.wait());
  ASSERT_TRUE(sync3->syncing);

  MockImageSync *sync4 = MockImageSync::instances[3];
  ASSERT_EQ(0, sync4->on_sync_start.wait());
  ASSERT_TRUE(sync4->syncing);

  MockImageSync *sync5 = MockImageSync::instances[4];
  ASSERT_FALSE(sync5->syncing);

  m_mock_instance_sync_throttler->set_max_concurrent_syncs(2);

  ASSERT_FALSE(sync5->syncing);

  sync1->finish(0);
  ASSERT_EQ(0, ctx1.wait());

  ASSERT_FALSE(sync5->syncing);

  sync2->finish(0);
  ASSERT_EQ(0, ctx2.wait());

  ASSERT_FALSE(sync5->syncing);

  sync3->finish(0);
  ASSERT_EQ(0, ctx3.wait());

  ASSERT_EQ(0, sync5->on_sync_start.wait());
  ASSERT_TRUE(sync5->syncing);

  sync4->finish(0);
  ASSERT_EQ(0, ctx4.wait());

  sync5->finish(0);
  ASSERT_EQ(0, ctx5.wait());
}


} // namespace mirror
} // namespace rbd

