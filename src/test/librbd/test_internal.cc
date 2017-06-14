// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd/librbd.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/api/DiffIterate.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "osdc/Striper.h"
#include <boost/scope_exit.hpp>
#include <boost/assign/list_of.hpp>
#include <atomic>
#include <utility>
#include <vector>

void register_test_internal() {
}

class TestInternal : public TestFixture {
public:

  TestInternal() {}

  typedef std::vector<std::pair<std::string, bool> > Snaps;

  void TearDown() override {
    unlock_image();
    for (Snaps::iterator iter = m_snaps.begin(); iter != m_snaps.end(); ++iter) {
      librbd::ImageCtx *ictx;
      EXPECT_EQ(0, open_image(m_image_name, &ictx));
      if (iter->second) {
	EXPECT_EQ(0,
		  ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(),
						   iter->first.c_str()));
      }
      EXPECT_EQ(0,
		ictx->operations->snap_remove(cls::rbd::UserSnapshotNamespace(),
					      iter->first.c_str()));
    }

    TestFixture::TearDown();
  }

  int create_snapshot(const char *snap_name, bool snap_protect) {
    librbd::ImageCtx *ictx;
    int r = open_image(m_image_name, &ictx);
    if (r < 0) {
      return r;
    }

    r = snap_create(*ictx, snap_name);
    if (r < 0) {
      return r;
    }

    m_snaps.push_back(std::make_pair(snap_name, snap_protect));
    if (snap_protect) {
      r = ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(), snap_name);
      if (r < 0) {
	return r;
      }
    }
    close_image(ictx);
    return 0;
  }

  Snaps m_snaps;
};

class DummyContext : public Context {
public:
  void finish(int r) override {
  }
};

void generate_random_iomap(librbd::Image &image, int num_objects, int object_size,
                           int max_count, map<uint64_t, uint64_t> &iomap)
{
  uint64_t stripe_unit, stripe_count;

  stripe_unit = image.get_stripe_unit();
  stripe_count = image.get_stripe_count();

  while (max_count-- > 0) {
    // generate random image offset based on base random object
    // number and object offset and then map that back to an
    // object number based on stripe unit and count.
    uint64_t ono = rand() % num_objects;
    uint64_t offset = rand() % (object_size - TEST_IO_SIZE);
    uint64_t imageoff = (ono * object_size) + offset;

    file_layout_t layout;
    layout.object_size = object_size;
    layout.stripe_unit = stripe_unit;
    layout.stripe_count = stripe_count;

    vector<ObjectExtent> ex;
    Striper::file_to_extents(g_ceph_context, 1, &layout, imageoff, TEST_IO_SIZE, 0, ex);

    // lets not worry if IO spans multiple extents (>1 object). in such
    // as case we would perform the write multiple times to the same
    // offset, but we record all objects that would be generated with
    // this IO. TODO: fix this if such a need is required by your
    // test.
    vector<ObjectExtent>::iterator it;
    map<uint64_t, uint64_t> curr_iomap;
    for (it = ex.begin(); it != ex.end(); ++it) {
      if (iomap.find((*it).objectno) != iomap.end()) {
        break;
      }

      curr_iomap.insert(make_pair((*it).objectno, imageoff));
    }

    if (it == ex.end()) {
      iomap.insert(curr_iomap.begin(), curr_iomap.end());
    }
  }
}

TEST_F(TestInternal, OpenByID) {
   REQUIRE_FORMAT_V2();

   librbd::ImageCtx *ictx;
   ASSERT_EQ(0, open_image(m_image_name, &ictx));
   std::string id = ictx->id;
   close_image(ictx);

   ictx = new librbd::ImageCtx("", id, nullptr, m_ioctx, true);
   ASSERT_EQ(0, ictx->state->open(false));
   ASSERT_EQ(ictx->name, m_image_name);
   close_image(ictx);
}

TEST_F(TestInternal, IsExclusiveLockOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);

  C_SaferCond ctx;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->exclusive_lock->try_acquire_lock(&ctx);
  }
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, ResizeLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->resize(m_image_size >> 1, true, no_op));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, ResizeFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EROFS, ictx->operations->resize(m_image_size >> 1, true, no_op));
}

TEST_F(TestInternal, SnapCreateLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  BOOST_SCOPE_EXIT( (ictx) ) {
    ASSERT_EQ(0,
	      ictx->operations->snap_remove(cls::rbd::UserSnapshotNamespace(),
					    "snap1"));
  } BOOST_SCOPE_EXIT_END;

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, SnapCreateFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  ASSERT_EQ(-EROFS, snap_create(*ictx, "snap1"));
}

TEST_F(TestInternal, SnapRollbackLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(),
					       "snap1",
					       no_op));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, SnapRollbackFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);


  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EROFS,
	    ictx->operations->snap_rollback(cls::rbd::UserSnapshotNamespace(),
					    "snap1",
					    no_op));
}

TEST_F(TestInternal, SnapSetReleasesLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_set(ictx, cls::rbd::UserSnapshotNamespace(), "snap1"));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);
}

TEST_F(TestInternal, FlattenLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1", true));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx2->operations->flatten(no_op));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx2, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, FlattenFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1", true));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  TestInternal *parent = this;
  librbd::ImageCtx *ictx2 = NULL;
  BOOST_SCOPE_EXIT( (&m_ioctx) (clone_name) (parent) (&ictx2) ) {
    if (ictx2 != NULL) {
      parent->close_image(ictx2);
      parent->unlock_image();
    }
    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, librbd::remove(m_ioctx, clone_name, "", no_op));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, lock_image(*ictx2, LOCK_EXCLUSIVE, "manually locked"));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EROFS, ictx2->operations->flatten(no_op));
}

TEST_F(TestInternal, AioWriteRequestsLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  std::string buffer(256, '1');
  Context *ctx = new DummyContext();
  auto c = librbd::io::AioCompletion::create(ctx);
  c->get();

  bufferlist bl;
  bl.append(buffer);
  ictx->io_work_queue->aio_write(c, 0, buffer.size(), std::move(bl), 0);

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);
  ASSERT_FALSE(c->is_complete());

  unlock_image();
  ASSERT_EQ(0, c->wait_for_complete());
  c->put();
}

TEST_F(TestInternal, AioDiscardRequestsLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  Context *ctx = new DummyContext();
  auto c = librbd::io::AioCompletion::create(ctx);
  c->get();
  ictx->io_work_queue->aio_discard(c, 0, 256, false);

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);
  ASSERT_FALSE(c->is_complete());

  unlock_image();
  ASSERT_EQ(0, c->wait_for_complete());
  c->put();
}

TEST_F(TestInternal, CancelAsyncResize) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  C_SaferCond ctx;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->exclusive_lock->try_acquire_lock(&ctx);
  }

  ASSERT_EQ(0, ctx.wait());
  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    ASSERT_TRUE(ictx->exclusive_lock->is_lock_owner());
  }

  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));

  uint32_t attempts = 0;
  while (attempts++ < 20 && size > 0) {
    C_SaferCond ctx;
    librbd::NoOpProgressContext prog_ctx;

    size -= MIN(size, 1<<18);
    {
      RWLock::RLocker l(ictx->owner_lock);
      ictx->operations->execute_resize(size, true, prog_ctx, &ctx, 0);
    }

    // try to interrupt the in-progress resize
    ictx->cancel_async_requests();

    int r = ctx.wait();
    if (r == -ERESTART) {
      std::cout << "detected canceled async request" << std::endl;
      break;
    }
    ASSERT_EQ(0, r);
  }
}

TEST_F(TestInternal, MultipleResize) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->exclusive_lock != nullptr) {
    C_SaferCond ctx;
    {
      RWLock::WLocker l(ictx->owner_lock);
      ictx->exclusive_lock->try_acquire_lock(&ctx);
    }

    RWLock::RLocker owner_locker(ictx->owner_lock);
    ASSERT_EQ(0, ctx.wait());
    ASSERT_TRUE(ictx->exclusive_lock->is_lock_owner());
  }

  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  uint64_t original_size = size;

  std::vector<C_SaferCond*> contexts;

  uint32_t attempts = 0;
  librbd::NoOpProgressContext prog_ctx;
  while (size > 0) {
    uint64_t new_size = original_size;
    if (attempts++ % 2 == 0) {
      size -= MIN(size, 1<<18);
      new_size = size;
    }

    RWLock::RLocker l(ictx->owner_lock);
    contexts.push_back(new C_SaferCond());
    ictx->operations->execute_resize(new_size, true, prog_ctx, contexts.back(), 0);
  }

  for (uint32_t i = 0; i < contexts.size(); ++i) {
    ASSERT_EQ(0, contexts[i]->wait());
    delete contexts[i];
  }

  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0U, size);
}

TEST_F(TestInternal, Metadata) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  map<string, bool> test_confs = boost::assign::map_list_of(
    "aaaaaaa", false)(
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false)(
    "cccccccccccccc", false);
  map<string, bool>::iterator it = test_confs.begin();
  int r;
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  r = ictx->operations->metadata_set(it->first, "value1");
  ASSERT_EQ(0, r);
  ++it;
  r = ictx->operations->metadata_set(it->first, "value2");
  ASSERT_EQ(0, r);
  ++it;
  r = ictx->operations->metadata_set(it->first, "value3");
  ASSERT_EQ(0, r);
  r = ictx->operations->metadata_set("abcd", "value4");
  ASSERT_EQ(0, r);
  r = ictx->operations->metadata_set("xyz", "value5");
  ASSERT_EQ(0, r);
  map<string, bufferlist> pairs;
  r = librbd::metadata_list(ictx, "", 0, &pairs);
  ASSERT_EQ(0, r);
  ASSERT_EQ(5u, pairs.size());
  r = ictx->operations->metadata_remove("abcd");
  ASSERT_EQ(0, r);
  r = ictx->operations->metadata_remove("xyz");
  ASSERT_EQ(0, r);
  pairs.clear();
  r = librbd::metadata_list(ictx, "", 0, &pairs);
  ASSERT_EQ(0, r);
  ASSERT_EQ(3u, pairs.size());
  string val;
  r = librbd::metadata_get(ictx, it->first, &val);
  ASSERT_EQ(0, r);
  ASSERT_STREQ(val.c_str(), "value3");
}

TEST_F(TestInternal, MetadataFilter) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  map<string, bool> test_confs = boost::assign::map_list_of(
    "aaaaaaa", false)(
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false)(
    "cccccccccccccc", false);
  map<string, bool>::iterator it = test_confs.begin();
  const string prefix = "test_config_";
  bool is_continue;
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::Image image1;
  map<string, bufferlist> pairs, res;
  pairs["abc"].append("value");
  pairs["abcabc"].append("value");
  pairs[prefix+it->first].append("value1");
  ++it;
  pairs[prefix+it->first].append("value2");
  ++it;
  pairs[prefix+it->first].append("value3");
  pairs[prefix+"asdfsdaf"].append("value6");
  pairs[prefix+"zxvzxcv123"].append("value5");

  is_continue = ictx->_filter_metadata_confs(prefix, test_confs, pairs, &res);
  ASSERT_TRUE(is_continue);
  ASSERT_TRUE(res.size() == 3U);
  it = test_confs.begin();
  ASSERT_TRUE(res.count(it->first));
  ASSERT_TRUE(it->second);
  ++it;
  ASSERT_TRUE(res.count(it->first));
  ASSERT_TRUE(it->second);
  ++it;
  ASSERT_TRUE(res.count(it->first));
  ASSERT_TRUE(it->second);
  res.clear();

  pairs["zzzzzzzz"].append("value7");
  is_continue = ictx->_filter_metadata_confs(prefix, test_confs, pairs, &res);
  ASSERT_FALSE(is_continue);
  ASSERT_TRUE(res.size() == 3U);
}

TEST_F(TestInternal, SnapshotCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bufferlist bl;
  bl.append(std::string(256, '1'));
  ASSERT_EQ(256, ictx->io_work_queue->write(0, bl.length(), bufferlist{bl}, 0));

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0,
	    ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					   "snap1"));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, snap_create(*ictx2, "snap1"));
  ASSERT_EQ(0, snap_create(*ictx2, "snap2"));

  ASSERT_EQ(256, ictx2->io_work_queue->write(256, bl.length(), bufferlist{bl},
                                             0));

  librados::IoCtx snap_ctx;
  snap_ctx.dup(ictx2->data_ctx);
  snap_ctx.snap_set_read(CEPH_SNAPDIR);

  librados::snap_set_t snap_set;
  ASSERT_EQ(0, snap_ctx.list_snaps(ictx2->get_object_name(0), &snap_set));

  std::vector< std::pair<uint64_t,uint64_t> > expected_overlap =
    boost::assign::list_of(
      std::make_pair(0, 256))(
      std::make_pair(512, 2096640));
  ASSERT_EQ(2U, snap_set.clones.size());
  ASSERT_NE(CEPH_NOSNAP, snap_set.clones[0].cloneid);
  ASSERT_EQ(2U, snap_set.clones[0].snaps.size());
  ASSERT_EQ(expected_overlap, snap_set.clones[0].overlap);
  ASSERT_EQ(CEPH_NOSNAP, snap_set.clones[1].cloneid);

  bufferptr read_ptr(256);
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  std::list<std::string> snaps = {"snap1", "snap2", ""};
  librbd::io::ReadResult read_result{&read_bl};
  for (std::list<std::string>::iterator it = snaps.begin();
       it != snaps.end(); ++it) {
    const char *snap_name = it->empty() ? NULL : it->c_str();
    ASSERT_EQ(0, librbd::snap_set(ictx2,
				  cls::rbd::UserSnapshotNamespace(),
				  snap_name));

    ASSERT_EQ(256,
              ictx2->io_work_queue->read(0, 256,
                                         librbd::io::ReadResult{read_result},
                                         0));
    ASSERT_TRUE(bl.contents_equal(read_bl));

    ASSERT_EQ(256,
              ictx2->io_work_queue->read(256, 256,
                                         librbd::io::ReadResult{read_result},
                                         0));
    if (snap_name == NULL) {
      ASSERT_TRUE(bl.contents_equal(read_bl));
    } else {
      ASSERT_TRUE(read_bl.is_zero());
    }

    // verify the object map was properly updated
    if ((ictx2->features & RBD_FEATURE_OBJECT_MAP) != 0) {
      uint8_t state = OBJECT_EXISTS;
      if ((ictx2->features & RBD_FEATURE_FAST_DIFF) != 0 &&
          it != snaps.begin() && snap_name != NULL) {
        state = OBJECT_EXISTS_CLEAN;
      }

      librbd::ObjectMap<> object_map(*ictx2, ictx2->snap_id);
      C_SaferCond ctx;
      object_map.open(&ctx);
      ASSERT_EQ(0, ctx.wait());

      RWLock::WLocker object_map_locker(ictx2->object_map_lock);
      ASSERT_EQ(state, object_map[0]);
    }
  }
}

TEST_F(TestInternal, ResizeCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  m_image_name = get_temp_image_name();
  m_image_size = 1 << 14;

  uint64_t features = 0;
  get_features(&features);
  int order = 12;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, m_image_name.c_str(), m_image_size,
                             features, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ((ssize_t)bl.length(),
	      ictx->io_work_queue->write(i, bl.length(),
					 bufferlist{bl}, 0));
  }

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0,
	    ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					   "snap1"));

  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, snap_create(*ictx2, "snap1"));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  // verify full / partial object removal properly copyup
  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx2->operations->resize(m_image_size - (1 << order) - 32,
                                         true, no_op));
  ASSERT_EQ(0, ictx2->operations->resize(m_image_size - (2 << order) - 32,
                                         true, no_op));
  ASSERT_EQ(0, librbd::snap_set(ictx2,
				cls::rbd::UserSnapshotNamespace(),
				"snap1"));

  {
    // hide the parent from the snapshot
    RWLock::WLocker snap_locker(ictx2->snap_lock);
    ictx2->snap_info.begin()->second.parent = librbd::ParentInfo();
  }

  librbd::io::ReadResult read_result{&read_bl};
  for (size_t i = 2 << order; i < m_image_size; i += bl.length()) {
    ASSERT_EQ((ssize_t)bl.length(),
              ictx2->io_work_queue->read(i, bl.length(),
                                         librbd::io::ReadResult{read_result},
                                         0));
    ASSERT_TRUE(bl.contents_equal(read_bl));
  }
}

TEST_F(TestInternal, DiscardCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  CephContext* cct = reinterpret_cast<CephContext*>(_rados.cct());
  REQUIRE(!cct->_conf->rbd_skip_partial_discard);

  m_image_name = get_temp_image_name();
  m_image_size = 1 << 14;

  uint64_t features = 0;
  get_features(&features);
  int order = 12;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, m_image_name.c_str(), m_image_size,
                             features, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ((ssize_t)bl.length(),
	      ictx->io_work_queue->write(i, bl.length(),
					 bufferlist{bl}, 0));
  }

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0,
	    ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					   "snap1"));

  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, snap_create(*ictx2, "snap1"));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  ASSERT_EQ(static_cast<int>(m_image_size - 64),
            ictx2->io_work_queue->discard(32, m_image_size - 64, false));
  ASSERT_EQ(0, librbd::snap_set(ictx2,
				cls::rbd::UserSnapshotNamespace(),
				"snap1"));

  {
    // hide the parent from the snapshot
    RWLock::WLocker snap_locker(ictx2->snap_lock);
    ictx2->snap_info.begin()->second.parent = librbd::ParentInfo();
  }

  librbd::io::ReadResult read_result{&read_bl};
  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ((ssize_t)bl.length(),
              ictx2->io_work_queue->read(i, bl.length(),
                                         librbd::io::ReadResult{read_result},
                                         0));
    ASSERT_TRUE(bl.contents_equal(read_bl));
  }
}

TEST_F(TestInternal, ShrinkFlushesCache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  std::string buffer(4096, '1');

  // ensure write-path is initialized
  bufferlist write_bl;
  write_bl.append(buffer);
  ictx->io_work_queue->write(0, buffer.size(), bufferlist{write_bl}, 0);

  C_SaferCond cond_ctx;
  auto c = librbd::io::AioCompletion::create(&cond_ctx);
  c->get();
  ictx->io_work_queue->aio_write(c, 0, buffer.size(), bufferlist{write_bl}, 0);

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->resize(m_image_size >> 1, true, no_op));

  ASSERT_TRUE(c->is_complete());
  ASSERT_EQ(0, c->wait_for_complete());
  ASSERT_EQ(0, cond_ctx.wait());
  c->put();
}

TEST_F(TestInternal, ImageOptions) {
  rbd_image_options_t opts1 = NULL, opts2 = NULL;
  uint64_t uint64_val1 = 10, uint64_val2 = 0;
  std::string string_val1;

  librbd::image_options_create(&opts1);
  ASSERT_NE((rbd_image_options_t)NULL, opts1);
  ASSERT_TRUE(librbd::image_options_is_empty(opts1));

  ASSERT_EQ(-EINVAL, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_FEATURES,
	  &string_val1));
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val1));

  ASSERT_EQ(-EINVAL, librbd::image_options_set(opts1, RBD_IMAGE_OPTION_FEATURES,
	  string_val1));

  ASSERT_EQ(0, librbd::image_options_set(opts1, RBD_IMAGE_OPTION_FEATURES,
	  uint64_val1));
  ASSERT_FALSE(librbd::image_options_is_empty(opts1));
  ASSERT_EQ(0, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  librbd::image_options_create_ref(&opts2, opts1);
  ASSERT_NE((rbd_image_options_t)NULL, opts2);
  ASSERT_FALSE(librbd::image_options_is_empty(opts2));

  uint64_val2 = 0;
  ASSERT_NE(uint64_val1, uint64_val2);
  ASSERT_EQ(0, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  uint64_val2++;
  ASSERT_NE(uint64_val1, uint64_val2);
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val1));
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val2));
  ASSERT_EQ(0, librbd::image_options_set(opts2, RBD_IMAGE_OPTION_ORDER,
	  uint64_val2));
  ASSERT_EQ(0, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val1));
  ASSERT_EQ(0, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  librbd::image_options_destroy(opts1);

  uint64_val2++;
  ASSERT_NE(uint64_val1, uint64_val2);
  ASSERT_EQ(0, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  ASSERT_EQ(0, librbd::image_options_unset(opts2, RBD_IMAGE_OPTION_ORDER));
  ASSERT_EQ(-ENOENT, librbd::image_options_unset(opts2, RBD_IMAGE_OPTION_ORDER));

  librbd::image_options_clear(opts2);
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val2));
  ASSERT_TRUE(librbd::image_options_is_empty(opts2));

  librbd::image_options_destroy(opts2);
}

TEST_F(TestInternal, WriteFullCopyup) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->resize(1 << ictx->order, true, no_op));

  bufferlist bl;
  bl.append(std::string(1 << ictx->order, '1'));
  ASSERT_EQ((ssize_t)bl.length(),
            ictx->io_work_queue->write(0, bl.length(), bufferlist{bl}, 0));
  ASSERT_EQ(0, librbd::flush(ictx));

  ASSERT_EQ(0, create_snapshot("snap1", true));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), ictx->features, &order, 0, 0));

  TestInternal *parent = this;
  librbd::ImageCtx *ictx2 = NULL;
  BOOST_SCOPE_EXIT( (&m_ioctx) (clone_name) (parent) (&ictx2) ) {
    if (ictx2 != NULL) {
      ictx2->operations->snap_remove(cls::rbd::UserSnapshotNamespace(),
				     "snap1");
      parent->close_image(ictx2);
    }

    librbd::NoOpProgressContext remove_no_op;
    ASSERT_EQ(0, librbd::remove(m_ioctx, clone_name, "", remove_no_op));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, ictx2->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					      "snap1"));

  bufferlist write_full_bl;
  write_full_bl.append(std::string(1 << ictx2->order, '2'));
  ASSERT_EQ((ssize_t)write_full_bl.length(),
            ictx2->io_work_queue->write(0, write_full_bl.length(),
                                        bufferlist{write_full_bl}, 0));

  ASSERT_EQ(0, ictx2->operations->flatten(no_op));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  librbd::io::ReadResult read_result{&read_bl};
  ASSERT_EQ((ssize_t)read_bl.length(),
            ictx2->io_work_queue->read(0, read_bl.length(),
                                       librbd::io::ReadResult{read_result}, 0));
  ASSERT_TRUE(write_full_bl.contents_equal(read_bl));

  ASSERT_EQ(0, librbd::snap_set(ictx2,
				cls::rbd::UserSnapshotNamespace(),
				"snap1"));
  ASSERT_EQ((ssize_t)read_bl.length(),
            ictx2->io_work_queue->read(0, read_bl.length(),
                                       librbd::io::ReadResult{read_result}, 0));
  ASSERT_TRUE(bl.contents_equal(read_bl));
}

TEST_F(TestInternal, RemoveById) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  std::string image_id = ictx->id;
  close_image(ictx);

  librbd::NoOpProgressContext remove_no_op;
  ASSERT_EQ(0, librbd::remove(m_ioctx, "", image_id, remove_no_op));
}

static int iterate_cb(uint64_t off, size_t len, int exists, void *arg)
{
  interval_set<uint64_t> *diff = static_cast<interval_set<uint64_t> *>(arg);
  diff->insert(off, len);
  return 0;
}

TEST_F(TestInternal, DiffIterateCloneOverwrite) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::RBD rbd;
  librbd::Image image;
  uint64_t size = 20 << 20;
  int order = 0;

  ASSERT_EQ(0, rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  ASSERT_EQ(4096, image.write(0, 4096, bl));

  interval_set<uint64_t> one;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, false, false, iterate_cb,
                                   (void *)&one));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(m_ioctx, m_image_name.c_str(), "one", m_ioctx,
                         clone_name.c_str(), RBD_FEATURE_LAYERING, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "one"));
  ASSERT_EQ(0,
	    ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
					   "one"));

  // Simulate a client that doesn't support deep flatten (old librbd / krbd)
  // which will copy up the full object from the parent
  std::string oid = ictx->object_prefix + ".0000000000000000";
  librados::IoCtx io_ctx;
  io_ctx.dup(m_ioctx);
  io_ctx.selfmanaged_snap_set_write_ctx(ictx->snapc.seq, ictx->snaps);
  ASSERT_EQ(0, io_ctx.write(oid, bl, 4096, 4096));

  interval_set<uint64_t> diff;
  ASSERT_EQ(0, librbd::snap_set(ictx, cls::rbd::UserSnapshotNamespace(), "one"));
  ASSERT_EQ(0, librbd::api::DiffIterate<>::diff_iterate(
    ictx, cls::rbd::UserSnapshotNamespace(), nullptr, 0, size, true, false,
    iterate_cb, (void *)&diff));
  ASSERT_EQ(one, diff);
}

TEST_F(TestInternal, TestCoR)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  std::string config_value;
  ASSERT_EQ(0, _rados.conf_get("rbd_clone_copy_on_read", config_value));
  if (config_value == "false") {
    std::cout << "SKIPPING due to disabled rbd_copy_on_read" << std::endl;
    return;
  }

  m_image_name = get_temp_image_name();
  m_image_size = 4 << 20;

  int order = 12; // smallest object size is 4K
  uint64_t features;
  ASSERT_TRUE(get_features(&features));

  ASSERT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, m_image_name, m_image_size,
                                    features, false, &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));

  librbd::image_info_t info;
  ASSERT_EQ(0, image.stat(info, sizeof(info)));

  const int object_num = info.size / info.obj_size;
  printf("made parent image \"%s\": %ldK (%d * %" PRIu64 "K)\n", m_image_name.c_str(),
         (unsigned long)m_image_size, object_num, info.obj_size/1024);

  // write something into parent
  char test_data[TEST_IO_SIZE + 1];
  for (int i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';

  // generate a random map which covers every objects with random
  // offset
  map<uint64_t, uint64_t> write_tracker;
  generate_random_iomap(image, object_num, info.obj_size, 100, write_tracker);

  printf("generated random write map:\n");
  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr)
    printf("\t [%-8ld, %-8ld]\n",
           (unsigned long)itr->first, (unsigned long)itr->second);

  bufferlist bl;
  bl.append(test_data, TEST_IO_SIZE);

  printf("write data based on random map\n");
  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr) {
    printf("\twrite object-%-4ld\t\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.write(itr->second, TEST_IO_SIZE, bl));
  }

  bufferlist readbl;
  printf("verify written data by reading\n");
  {
    map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
    printf("\tread object-%-4ld\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.read(itr->second, TEST_IO_SIZE, readbl));
    ASSERT_TRUE(readbl.contents_equal(bl));
  }

  int64_t data_pool_id = image.get_data_pool_id();
  rados_ioctx_t d_ioctx;
  rados_ioctx_create2(_cluster, data_pool_id, &d_ioctx);

  const char *entry;
  rados_list_ctx_t list_ctx;
  set<string> obj_checker;
  ASSERT_EQ(0, rados_nobjects_list_open(d_ioctx, &list_ctx));
  while (rados_nobjects_list_next(list_ctx, &entry, NULL, NULL) != -ENOENT) {
    if (strstr(entry, info.block_name_prefix)) {
      const char *block_name_suffix = entry + strlen(info.block_name_prefix) + 1;
      obj_checker.insert(block_name_suffix);
    }
  }
  rados_nobjects_list_close(list_ctx);

  std::string snapname = "snap";
  std::string clonename = get_temp_image_name();
  ASSERT_EQ(0, image.snap_create(snapname.c_str()));
  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str(), snapname.c_str()));
  ASSERT_EQ(0, image.snap_protect(snapname.c_str()));
  printf("made snapshot \"%s@parent_snap\" and protect it\n", m_image_name.c_str());

  ASSERT_EQ(0, clone_image_pp(m_rbd, image, m_ioctx, m_image_name.c_str(), snapname.c_str(),
                              m_ioctx, clonename.c_str(), features));
  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, clonename.c_str(), NULL));
  printf("made and opened clone \"%s\"\n", clonename.c_str());

  printf("read from \"child\"\n");
  {
    map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
    printf("\tread object-%-4ld\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.read(itr->second, TEST_IO_SIZE, readbl));
    ASSERT_TRUE(readbl.contents_equal(bl));
  }

  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr) {
    printf("\tread object-%-4ld\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.read(itr->second, TEST_IO_SIZE, readbl));
    ASSERT_TRUE(readbl.contents_equal(bl));
  }

  printf("read again reversely\n");
  for (map<uint64_t, uint64_t>::iterator itr = --write_tracker.end();
       itr != write_tracker.begin(); --itr) {
    printf("\tread object-%-4ld\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.read(itr->second, TEST_IO_SIZE, readbl));
    ASSERT_TRUE(readbl.contents_equal(bl));
  }

  // close child to flush all copy-on-read
  ASSERT_EQ(0, image.close());

  printf("check whether child image has the same set of objects as parent\n");
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, clonename.c_str(), NULL));
  ASSERT_EQ(0, image.stat(info, sizeof(info)));

  ASSERT_EQ(0, rados_nobjects_list_open(d_ioctx, &list_ctx));
  while (rados_nobjects_list_next(list_ctx, &entry, NULL, NULL) != -ENOENT) {
    if (strstr(entry, info.block_name_prefix)) {
      const char *block_name_suffix = entry + strlen(info.block_name_prefix) + 1;
      set<string>::iterator it = obj_checker.find(block_name_suffix);
      ASSERT_TRUE(it != obj_checker.end());
      obj_checker.erase(it);
    }
  }
  rados_nobjects_list_close(list_ctx);
  ASSERT_TRUE(obj_checker.empty());
  ASSERT_EQ(0, image.close());

  rados_ioctx_destroy(d_ioctx);
}

TEST_F(TestInternal, FlattenNoEmptyObjects)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  m_image_name = get_temp_image_name();
  m_image_size = 4 << 20;

  int order = 12; // smallest object size is 4K
  uint64_t features;
  ASSERT_TRUE(get_features(&features));

  ASSERT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, m_image_name, m_image_size,
                                    features, false, &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));

  librbd::image_info_t info;
  ASSERT_EQ(0, image.stat(info, sizeof(info)));

  const int object_num = info.size / info.obj_size;
  printf("made parent image \"%s\": %" PRIu64 "K (%d * %" PRIu64 "K)\n",
	 m_image_name.c_str(), m_image_size, object_num, info.obj_size/1024);

  // write something into parent
  char test_data[TEST_IO_SIZE + 1];
  for (int i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';

  // generate a random map which covers every objects with random
  // offset
  map<uint64_t, uint64_t> write_tracker;
  generate_random_iomap(image, object_num, info.obj_size, 100, write_tracker);

  printf("generated random write map:\n");
  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr)
    printf("\t [%-8ld, %-8ld]\n",
           (unsigned long)itr->first, (unsigned long)itr->second);

  bufferlist bl;
  bl.append(test_data, TEST_IO_SIZE);

  printf("write data based on random map\n");
  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr) {
    printf("\twrite object-%-4ld\t\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.write(itr->second, TEST_IO_SIZE, bl));
  }

  bufferlist readbl;
  printf("verify written data by reading\n");
  {
    map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
    printf("\tread object-%-4ld\n", (unsigned long)itr->first);
    ASSERT_EQ(TEST_IO_SIZE, image.read(itr->second, TEST_IO_SIZE, readbl));
    ASSERT_TRUE(readbl.contents_equal(bl));
  }

  int64_t data_pool_id = image.get_data_pool_id();
  rados_ioctx_t d_ioctx;
  rados_ioctx_create2(_cluster, data_pool_id, &d_ioctx);

  const char *entry;
  rados_list_ctx_t list_ctx;
  set<string> obj_checker;
  ASSERT_EQ(0, rados_nobjects_list_open(d_ioctx, &list_ctx));
  while (rados_nobjects_list_next(list_ctx, &entry, NULL, NULL) != -ENOENT) {
    if (strstr(entry, info.block_name_prefix)) {
      const char *block_name_suffix = entry + strlen(info.block_name_prefix) + 1;
      obj_checker.insert(block_name_suffix);
    }
  }
  rados_nobjects_list_close(list_ctx);

  std::string snapname = "snap";
  std::string clonename = get_temp_image_name();
  ASSERT_EQ(0, image.snap_create(snapname.c_str()));
  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str(), snapname.c_str()));
  ASSERT_EQ(0, image.snap_protect(snapname.c_str()));
  printf("made snapshot \"%s@parent_snap\" and protect it\n", m_image_name.c_str());

  ASSERT_EQ(0, clone_image_pp(m_rbd, image, m_ioctx, m_image_name.c_str(), snapname.c_str(),
                              m_ioctx, clonename.c_str(), features));
  ASSERT_EQ(0, image.close());

  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, clonename.c_str(), NULL));
  printf("made and opened clone \"%s\"\n", clonename.c_str());

  printf("flattening clone: \"%s\"\n", clonename.c_str());
  ASSERT_EQ(0, image.flatten());

  printf("check whether child image has the same set of objects as parent\n");
  ASSERT_EQ(0, image.stat(info, sizeof(info)));

  ASSERT_EQ(0, rados_nobjects_list_open(d_ioctx, &list_ctx));
  while (rados_nobjects_list_next(list_ctx, &entry, NULL, NULL) != -ENOENT) {
    if (strstr(entry, info.block_name_prefix)) {
      const char *block_name_suffix = entry + strlen(info.block_name_prefix) + 1;
      set<string>::iterator it = obj_checker.find(block_name_suffix);
      ASSERT_TRUE(it != obj_checker.end());
      obj_checker.erase(it);
    }
  }
  rados_nobjects_list_close(list_ctx);
  ASSERT_TRUE(obj_checker.empty());
  ASSERT_EQ(0, image.close());

  rados_ioctx_destroy(d_ioctx);
}

TEST_F(TestInternal, BlockAPI) {
  REQUIRE_FORMAT_V2();

  // Create and open old format image

  uint64_t features = 0;
  int order = 0;
  uint64_t size = 4 << 20;
  std::string name = get_temp_image_name();
  ASSERT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, name, size, features, true,
                                    &order));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_ioctx.get_pool_name().c_str(), &ioctx);

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  rbd_image_info_t info;
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size);

  // Do some IO

  char test_data[TEST_IO_SIZE];
  for (size_t i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }

  size_t num_aios = 256;
  rbd_completion_t comps[num_aios];
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                               comps[i]));
  }
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  // Block API and close image state

  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  C_SaferCond on_block;
  ictx->block_api(&on_block);
  ASSERT_EQ(0, on_block.wait());

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_LE(0, rbd_aio_read(image, offset, TEST_IO_SIZE, test_data,
                              comps[i]));
  }

  C_SaferCond on_close;
  ictx->state->close(&on_close);
  ASSERT_EQ(0, on_close.wait());

  ictx->md_ctx.aio_flush();
  ictx->data_ctx.aio_flush();
  ictx->io_work_queue->drain();

  if (ictx->object_cacher) {
    delete ictx->object_cacher;
    ictx->object_cacher = NULL;
  }
  if (ictx->perfcounter) {
    ictx->perf_stop();
    ictx->perfcounter = NULL;
  }
  if (ictx->object_cacher) {
    delete ictx->object_cacher;
    ictx->object_cacher = NULL;
  }
  if (ictx->writeback_handler) {
    delete ictx->writeback_handler;
    ictx->writeback_handler = NULL;
  }
  if (ictx->object_set) {
    delete ictx->object_set;
    ictx->object_set = NULL;
  }
  delete[] ictx->format_string;
  ictx->format_string = NULL;

  ThreadPool *thread_pool;
  ictx->get_thread_pool_instance(ictx->cct, &thread_pool, &ictx->op_work_queue);

  delete ictx->io_work_queue;
  ictx->io_work_queue = new librbd::io::ImageRequestWQ(
    ictx, "librbd::io_work_queue", ictx->cct->_conf->rbd_op_thread_timeout,
    thread_pool);

  delete ictx->state;
  ictx->state = new librbd::ImageState<>(ictx);

  // Create new format image

  name = get_temp_image_name();
  size *= 2;
  order += 1;
  ASSERT_TRUE(get_features(&features));
  ASSERT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, name, size, features, false,
                                    &order));

  // Substitute old image with new one, open and unblock API

  ictx->name = name;
  ictx->id.clear();
  C_SaferCond on_open;
  ictx->state->open(true, &on_open);
  ASSERT_EQ(0, on_open.wait());

  ictx->unblock_api();

  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size);

  // Release completions for aio started before block/unblock

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  // Do some IO

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                               comps[i]));
  }
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}


TEST_F(TestInternal, BlockAPIStress) {
  REQUIRE_FORMAT_V2();

  rbd_image_t image;
  std::atomic<bool> done(false);

  class Thrasher : public Thread {
  public:
    Thrasher(TestInternal *test, rbd_image_t &image, std::atomic<bool> &done)
      : m_test(test), m_image(image), m_done(done) {
    };

  protected:
    void *entry() override {
      int n = 0;
      while (!m_done) {
        // Block API and close image state

        rbd_image_info_t info;
        EXPECT_EQ(0, rbd_stat(m_image, &info, sizeof(info)));

        librbd::ImageCtx *ictx = (librbd::ImageCtx *)m_image;

        C_SaferCond on_block;
        ictx->block_api(&on_block);
        EXPECT_EQ(0, on_block.wait());

        C_SaferCond on_close;
        ictx->state->close(&on_close);
        EXPECT_EQ(0, on_close.wait());

        ictx->md_ctx.aio_flush();
        ictx->data_ctx.aio_flush();
        ictx->io_work_queue->drain();

        if (ictx->object_cacher) {
          delete ictx->object_cacher;
          ictx->object_cacher = NULL;
        }
        if (ictx->perfcounter) {
          ictx->perf_stop();
          ictx->perfcounter = NULL;
        }
        if (ictx->object_cacher) {
          delete ictx->object_cacher;
          ictx->object_cacher = NULL;
        }
        if (ictx->writeback_handler) {
          delete ictx->writeback_handler;
          ictx->writeback_handler = NULL;
        }
        if (ictx->object_set) {
          delete ictx->object_set;
          ictx->object_set = NULL;
        }
        delete[] ictx->format_string;
        ictx->format_string = NULL;

        ThreadPool *thread_pool;
        ictx->get_thread_pool_instance(ictx->cct, &thread_pool,
                                       &ictx->op_work_queue);

        delete ictx->io_work_queue;
        ictx->io_work_queue = new librbd::io::ImageRequestWQ(
          ictx, "librbd::io_work_queue",
          ictx->cct->_conf->rbd_op_thread_timeout, thread_pool);

        delete ictx->state;
        ictx->state = new librbd::ImageState<>(ictx);

        // Create a new image

        uint64_t features;
        int order = 0;
        uint64_t size = info.size * 2;
        std::string name = get_temp_image_name();
        EXPECT_TRUE(get_features(&features));
        EXPECT_EQ(0, create_image_full_pp(m_test->m_rbd, m_test->m_ioctx, name,
                                          size, features, false, &order));

        // Substitute the old image with the new one, open and unblock API

        EXPECT_EQ(0, m_test->m_rbd.remove(m_test->m_ioctx, ictx->name.c_str()));

        ictx->name = name;
        ictx->id.clear();
        C_SaferCond on_open;
        ictx->state->open(true, &on_open);
        EXPECT_EQ(0, on_open.wait());

        ictx->unblock_api();

        EXPECT_EQ(0, rbd_stat(m_image, &info, sizeof(info)));
        EXPECT_EQ(info.size, size);

        usleep(rand() % 40000);
        n++;
        if (n % 100 == 0) {
          std::cout << "n=" << n << std::endl;
        }
      }

      std::cout << "n=" << n << std::endl;

      return nullptr;
    }

  private:
    TestInternal *m_test;
    rbd_image_t &m_image;
    std::atomic<bool> &m_done;
  } thrasher(this, image, done);

  uint64_t features;
  int order = 0;
  uint64_t size = 4 << 20;
  std::string name = get_temp_image_name();
  ASSERT_TRUE(get_features(&features));
  ASSERT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, name, size, features, false,
                                    &order));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_ioctx.get_pool_name().c_str(), &ioctx);

  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  thrasher.create("thrasher");

  for (int i = 0; i < 1000; i++) {
    rbd_image_info_t info;
    ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));

    char test_data[TEST_IO_SIZE];
    for (size_t i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }

    size_t num_aios = 256;
    rbd_completion_t comps[num_aios];
    for (size_t i = 0; i < num_aios; ++i) {
      ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                                 comps[i]));
    }
    for (size_t i = 0; i < num_aios; ++i) {
      ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
      rbd_aio_release(comps[i]);
    }


    for (size_t i = 0; i < num_aios; ++i) {
      ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_LE(0, rbd_aio_read(image, offset, TEST_IO_SIZE, test_data,
                                comps[i]));
    }

    for (size_t i = 0; i < num_aios; ++i) {
      ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
      rbd_aio_release(comps[i]);
    }

    ASSERT_EQ(0, rbd_invalidate_cache(image));

    uint64_t size = 4 << (19 + (rand() % 2));
    ASSERT_EQ(0, rbd_resize(image, size));

    ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));

    for (size_t i = 0; i < num_aios; ++i) {
      ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                                 comps[i]));
    }

    for (size_t i = 0; i < num_aios; ++i) {
      ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
      rbd_aio_release(comps[i]);
    }

    ASSERT_EQ(0, rbd_get_features(image, &features));

    uint64_t disable_features;
    disable_features = features & (RBD_FEATURE_EXCLUSIVE_LOCK |
                                   RBD_FEATURE_OBJECT_MAP |
                                   RBD_FEATURE_FAST_DIFF |
                                   RBD_FEATURE_JOURNALING);

    rbd_update_features(image, disable_features, false);
    rbd_update_features(image, features, true);

    rbd_snap_info_t snaps[10];
    int max_size = 10;
    ASSERT_GE(rbd_snap_list(image, snaps, &max_size), 0);

    ASSERT_EQ(0, rbd_flush(image));

    if (i % 100 == 0) {
      std::cout << "i=" << i << std::endl;
    }

  }

  done = true;
  thrasher.join();

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestInternal, LiveConvertToNewFormat) {
  REQUIRE_FORMAT_V2();

  // Create and open old format image

  uint64_t features = 0;
  int order = 0;
  uint64_t size = 4 << 20;
  std::string name = get_temp_image_name();
  ASSERT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, name, size, features, true,
                                    &order));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_ioctx.get_pool_name().c_str(), &ioctx);

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  rbd_image_info_t info;
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size);

  // Do some IO

  char test_data[TEST_IO_SIZE];
  char read_data[TEST_IO_SIZE];
  for (size_t i = 0; i < TEST_IO_SIZE; ++i) {
    //test_data[i] = (char) (rand() % (126 - 33) + 33);
    test_data[i] = 'X';
  }
  test_data[TEST_IO_SIZE - 1] = '\0';

  size_t num_aios = 4;
  rbd_completion_t comps[num_aios];
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                               comps[i]));
  }
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(TEST_IO_SIZE, rbd_write(image, i * TEST_IO_SIZE, TEST_IO_SIZE, test_data));
  }

  // Block API and close image state
  std::cout << "Block API and close image state" << std::endl;

  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;

  C_SaferCond on_block;
  ictx->block_api(&on_block);
  ASSERT_EQ(0, on_block.wait());

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_LE(0, rbd_aio_read(image, offset, TEST_IO_SIZE, read_data,
                              comps[i]));
  }

  C_SaferCond on_close;
  ictx->state->close(&on_close);
  ASSERT_EQ(0, on_close.wait());

  ictx->md_ctx.aio_flush();
  ictx->data_ctx.aio_flush();
  ictx->io_work_queue->drain();

  if (ictx->object_cacher) {
    delete ictx->object_cacher;
    ictx->object_cacher = NULL;
  }
  if (ictx->perfcounter) {
    ictx->perf_stop();
    ictx->perfcounter = NULL;
  }
  if (ictx->object_cacher) {
    delete ictx->object_cacher;
    ictx->object_cacher = NULL;
  }
  if (ictx->writeback_handler) {
    delete ictx->writeback_handler;
    ictx->writeback_handler = NULL;
  }
  if (ictx->object_set) {
    delete ictx->object_set;
    ictx->object_set = NULL;
  }
  delete[] ictx->format_string;
  ictx->format_string = NULL;

  ThreadPool *thread_pool;
  ictx->get_thread_pool_instance(ictx->cct, &thread_pool, &ictx->op_work_queue);

  delete ictx->io_work_queue;
  ictx->io_work_queue = new librbd::io::ImageRequestWQ(
    ictx, "librbd::io_work_queue", ictx->cct->_conf->rbd_op_thread_timeout,
    thread_pool);

  delete ictx->state;
  ictx->state = new librbd::ImageState<>(ictx);

  // librbd::ImageCtx *parent_ictx;
  // EXPECT_EQ(0, open_image(ictx->name, &parent_ictx));

  // unlink old image in directory
  ASSERT_EQ(0, librbd::tmap_rm(ictx->md_ctx, ictx->name));

  // rename old image header
  name = get_temp_image_name();

  bufferlist header;
  ASSERT_EQ(0, librbd::read_header_bl(ictx->md_ctx, ictx->header_oid, header,
                                      nullptr));

  string old_header_oid = librbd::util::old_header_name(name);
  ASSERT_EQ(0, ictx->md_ctx.write(old_header_oid, header, header.length(), 0));
  ASSERT_EQ(0, ictx->md_ctx.remove(ictx->header_oid));

  std::cout << "Create new image and set old as parent" << std::endl;

  // create new image

  ASSERT_TRUE(get_features(&features));
  // librbd::ImageOptions opts;
  // opts.set(RBD_IMAGE_OPTION_FORMAT, static_cast<uint64_t>(2));
  // opts.set(RBD_IMAGE_OPTION_ORDER, static_cast<uint64_t>(22));
  // opts.set(RBD_IMAGE_OPTION_FEATURES, features);

  // C_SaferCond on_create;
  // auto req = librbd::image::CreateRequest<>::create(
  //     m_ioctx, ictx->name, "", size, opts, "", "", true, ictx->op_work_queue,
  //     &on_create);
  // // req->set_object_prefix(ictx->object_prefix);
  // req->send();
  // ASSERT_EQ(0, on_create.wait());

  //name = get_temp_image_name();
  EXPECT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, ictx->name, size, features,
                                    false, &order));

  // get new image header_oid
  librbd::ImageCtx *ictx2;
  EXPECT_EQ(0, open_image(ictx->name, &ictx2));
  std::string header_oid = ictx2->header_oid;
  close_image(ictx2);

  // set old image as parent
  librbd::ParentSpec pspec(ictx->md_ctx.get_id(), name /* XXXMG: old image name */, CEPH_NOSNAP);
  librados::ObjectWriteOperation op1;
  librbd::cls_client::set_parent(&op1, pspec, size);
  std::cout << "header_oid=" << header_oid << std::endl;
  ASSERT_EQ(0, ictx->md_ctx.operate(header_oid, &op1));

  // Reopen and unblock API
  std::cout << "Reopen and unblock API" << std::endl;

  //ictx->name = name;
  ictx->id.clear();
  C_SaferCond on_open;
  ictx->state->open(false, &on_open);
  ASSERT_EQ(0, on_open.wait());

  ictx->unblock_api();

  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size);

  // Release completions for aio started before block/unblock
  std::cout << "Release completions for aio started before block/unblock" << std::endl;

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  // check written data
  std::cout << "Check written data" << std::endl;
  for (size_t i = 0; i < num_aios; ++i) {
    read_data[0] = '\0';
    std::cout << "read, i = " << i << std::endl;
    ASSERT_EQ(TEST_IO_SIZE, rbd_read(image, i * TEST_IO_SIZE, TEST_IO_SIZE, read_data));
    std::cout << "test_data = " << std::string(test_data) << std::endl;
    std::cout << "read_data = " << std::string(read_data) << std::endl;
    ASSERT_EQ(0, memcmp(read_data, test_data, TEST_IO_SIZE));
  }

  // Do some IO
  std::cout << "Do some IO" << std::endl;

  for (size_t i = num_aios; i < 2 * num_aios; ++i) {
    std::cout << "write, i = " << i << std::endl;
    ASSERT_EQ(TEST_IO_SIZE, rbd_write(image, i * TEST_IO_SIZE, TEST_IO_SIZE, test_data));
  }
  for (size_t i = num_aios; i < 2 * num_aios; ++i) {
    std::cout << "read, i = " << i << std::endl;
    ASSERT_EQ(TEST_IO_SIZE, rbd_read(image, i * TEST_IO_SIZE, TEST_IO_SIZE, read_data));
    ASSERT_EQ(0, memcmp(read_data, test_data, TEST_IO_SIZE));
  }

  for (size_t i = 0; i < num_aios; ++i) {
    std::cout << "i = " << i << std::endl;
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                               comps[i]));
  }
  std::cout << "Wait for complete" << std::endl;
  for (size_t i = 0; i < num_aios; ++i) {
    std::cout << "i = " << i << std::endl;
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  std::cout << "Flatten" << std::endl;
  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->flatten(no_op));

  std::cout << "Close" << std::endl;
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}
