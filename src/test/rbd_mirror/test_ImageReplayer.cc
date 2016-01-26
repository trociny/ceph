// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis Inc
 *
 * Author: Mykola Golub <mgolub@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"
#include "journal/Journaler.h"
#include "librbd/AioImageRequestWQ.h"
#include "tools/rbd_mirror/types.h"
#include "tools/rbd_mirror/ImageReplayer.h"

void register_test_rbd_mirror() {
}

#define TEST_IO_SIZE 512

class TestImageReplayer : public TestFixture {
public:
  struct C_WatchCtx : public librados::WatchCtx2 {
    TestImageReplayer *test;
    Mutex lock;
    Cond cond;
    bool notified;

    C_WatchCtx(TestImageReplayer *test)
      : test(test), lock("C_WatchCtx::lock"), notified(false) {
    }


    virtual void handle_notify(uint64_t notify_id, uint64_t cookie,
                               uint64_t notifier_id, bufferlist& bl_) {
      bufferlist bl;
      test->m_ioctx.notify_ack(test->m_journal_id, notify_id, cookie, bl);

      Mutex::Locker locker(lock);
      notified = true;
      cond.Signal();
    }

    virtual void handle_error(uint64_t cookie, int err) {
      ASSERT_EQ(0, err);
    }
  };

  TestImageReplayer() : m_replayer(nullptr), m_watch_handle(0)
  {
  }

  ~TestImageReplayer()
  {
    delete m_replayer;
  }

  virtual void SetUp()
  {
    setenv("RBD_FEATURES", "109", 0);

    TestFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    m_journal_id = ictx->id;
    close_image(ictx);

    m_local_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, m_local_image_name,
				 m_image_size));

    m_replayer = new rbd::mirror::ImageReplayer(
      rbd::mirror::RadosRef(new librados::Rados(m_ioctx)),
      rbd::mirror::RadosRef(new librados::Rados(m_ioctx)),
      _pool_name, _pool_name, m_local_image_name, m_image_name);

    m_watch_ctx = new C_WatchCtx(this);
    std::string oid = ::journal::Journaler::header_oid(m_journal_id);
    ASSERT_EQ(0, m_ioctx.watch2(oid, &m_watch_handle, m_watch_ctx));
  }

  virtual void TearDown()
  {
    if (m_watch_handle != 0) {
      m_ioctx.unwatch2(m_watch_handle);
    }

    TestFixture::TearDown();
  }

  void get_commit_positions(int64_t *master_tid_p, int64_t *mirror_tid_p)
  {
    const std::string MASTER_CLIENT_ID = "";
    const std::string MIRROR_CLIENT_ID = "MIRROR";
    const std::string TAG = "";

    C_SaferCond cond;
    uint64_t minimum_set;
    uint64_t active_set;
    std::set<cls::journal::Client> registered_clients;
    std::string oid = ::journal::Journaler::header_oid(m_journal_id);
    cls::journal::client::get_mutable_metadata(m_ioctx, oid, &minimum_set,
	&active_set, &registered_clients, &cond);
    ASSERT_EQ(0, cond.wait());

    int64_t master_tid = -1;
    int64_t mirror_tid = -1;

    std::set<cls::journal::Client>::const_iterator c;
    for (c = registered_clients.begin(); c != registered_clients.end(); c++) {
      std::cout << __func__ << ": client: " << *c << std::endl;
      cls::journal::EntryPositions entry_positions =
	c->commit_position.entry_positions;
      cls::journal::EntryPositions::const_iterator p;
      for (p = entry_positions.begin(); p != entry_positions.end(); p++) {
	if (p->tag != TAG) {
	  continue;
	}
	if (c->id == MASTER_CLIENT_ID) {
	  ASSERT_EQ(-1, master_tid);
	  master_tid = p->tid;
	} else if (c->id == MIRROR_CLIENT_ID) {
	  ASSERT_EQ(-1, mirror_tid);
	  mirror_tid = p->tid;
	}
      }
    }

    *master_tid_p = master_tid;
    *mirror_tid_p = mirror_tid;
  }

  bool wait_for_watcher_notify() {
    Mutex::Locker locker(m_watch_ctx->lock);
    while (!m_watch_ctx->notified) {
      if (m_watch_ctx->cond.WaitInterval(g_ceph_context, m_watch_ctx->lock,
					 utime_t(10, 0)) != 0) {
        return false;
      }
    }
    m_watch_ctx->notified = false;
    return true;
  }

  void wait_for_replay_complete()
  {
    int64_t master_tid;
    int64_t mirror_tid;

    for (int i = 0; i < 10; i++) {
      ASSERT_TRUE(wait_for_watcher_notify());
      get_commit_positions(&master_tid, &mirror_tid);
      if (master_tid == mirror_tid) {
	break;
      }
    }

    ASSERT_EQ(master_tid, mirror_tid);
  }

  void write_test_data(librbd::ImageCtx *ictx, const char *test_data, off_t off,
                       size_t len)
  {
    size_t written;
    written = ictx->aio_work_queue->write(off, len, test_data, 0);
    printf("wrote: %d\n", (int)written);
    ASSERT_EQ(len, written);
  }

  void read_test_data(librbd::ImageCtx *ictx, const char *expected, off_t off,
                      size_t len)
  {
    ssize_t read;
    char *result = (char *)malloc(len + 1);

    ASSERT_NE(static_cast<char *>(NULL), result);
    read = ictx->aio_work_queue->read(off, len, result, 0);
    printf("read: %d\n", (int)read);
    ASSERT_EQ(len, static_cast<size_t>(read));
    result[len] = '\0';
    if (memcmp(result, expected, len)) {
      printf("read: %s\nexpected: %s\n", result, expected);
      ASSERT_EQ(0, memcmp(result, expected, len));
    }
    free(result);
  }

  void generate_test_data() {
    for (int i = 0; i < TEST_IO_SIZE; ++i) {
      m_test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    m_test_data[TEST_IO_SIZE] = '\0';
  }

  std::string m_journal_id;
  std::string m_local_image_name;
  rbd::mirror::ImageReplayer *m_replayer;
  C_WatchCtx *m_watch_ctx;
  uint64_t m_watch_handle;
  char m_test_data[TEST_IO_SIZE + 1];
};

TEST_F(TestImageReplayer, StartStop)
{
  ASSERT_EQ(0, m_replayer->start());
  wait_for_replay_complete();
  m_replayer->stop();
}

TEST_F(TestImageReplayer, WriteAndStartReplay)
{
  // Write to remote image and start replay

  librbd::ImageCtx *ictx;

  generate_test_data();
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  for (int i = 0; i < 5; ++i) {
    write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  close_image(ictx);

  ASSERT_EQ(0, m_replayer->start());
  wait_for_replay_complete();
  m_replayer->stop();

  ASSERT_EQ(0, open_image(m_local_image_name, &ictx));
  for (int i = 0; i < 5; ++i) {
    read_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  close_image(ictx);
}

TEST_F(TestImageReplayer, StartReplayAndWrite)
{
  // Start replay and write to remote image

  librbd::ImageCtx *ictx;

  ASSERT_EQ(0, m_replayer->start());

  generate_test_data();
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  for (int i = 0; i < 5; ++i) {
    write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }

  wait_for_replay_complete();

  for (int i = 5; i < 10; ++i) {
    write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }

  close_image(ictx);

  wait_for_replay_complete();
  m_replayer->stop();

  ASSERT_EQ(0, open_image(m_local_image_name, &ictx));
  for (int i = 0; i < 10; ++i) {
    read_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  close_image(ictx);
}
