// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/Operations.h"
#include "librbd/api/Image.h"
#include "librbd/internal.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ReadResult.h"

void register_test_deep_copy() {
}

struct TestDeepCopy : public TestFixture {
  void SetUp() override {
    TestFixture::SetUp();

    std::string image_name = get_temp_image_name();
    int order = 22;
    uint64_t size = (1 << order) * 20;
    uint64_t features = 0;
    bool old_format = !get_features(&features);
    EXPECT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, image_name, size,
                                      features, old_format, &order));
    ASSERT_EQ(0, open_image(image_name, &m_src_ictx));

    if (old_format) {
      // The destination should always be in the new format.
      uint64_t format = 2;
      ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FORMAT, format));
    }
  }

  void TearDown() override {
    if (m_src_ictx != nullptr) {
      deep_copy();
      if (m_dst_ictx != nullptr) {
        compare();
        close_image(m_dst_ictx);
      }
      close_image(m_src_ictx);
    }

    TestFixture::TearDown();
  }

  void deep_copy() {
    std::string dst_name = get_temp_image_name();
    librbd::NoOpProgressContext no_op;
    EXPECT_EQ(0, m_src_ictx->io_work_queue->flush());
    EXPECT_EQ(0, librbd::api::Image<>::deep_copy(m_src_ictx, m_src_ictx->md_ctx,
                                                 dst_name.c_str(), m_opts,
                                                 no_op));
    EXPECT_EQ(0, open_image(dst_name, &m_dst_ictx));
  }

  void compare() {
    vector<librbd::snap_info_t> src_snaps, dst_snaps;

    EXPECT_EQ(m_src_ictx->size, m_dst_ictx->size);
    EXPECT_EQ(0, librbd::snap_list(m_src_ictx, src_snaps));
    EXPECT_EQ(0, librbd::snap_list(m_dst_ictx, dst_snaps));
    EXPECT_EQ(src_snaps.size(), dst_snaps.size());
    for (size_t i = 0; i <= src_snaps.size(); i++) {
      const char *src_snap_name = nullptr;
      const char *dst_snap_name = nullptr;
      if (i < src_snaps.size()) {
        EXPECT_EQ(src_snaps[i].name, dst_snaps[i].name);
        src_snap_name = src_snaps[i].name.c_str();
        dst_snap_name = dst_snaps[i].name.c_str();
      }
      EXPECT_EQ(0, librbd::api::Image<>::snap_set(
                     m_src_ictx, cls::rbd::UserSnapshotNamespace(),
                     src_snap_name));
      EXPECT_EQ(0, librbd::api::Image<>::snap_set(
                     m_dst_ictx, cls::rbd::UserSnapshotNamespace(),
                     dst_snap_name));
      uint64_t src_size, dst_size;
      {
        RWLock::RLocker src_locker(m_src_ictx->snap_lock);
        RWLock::RLocker dst_locker(m_dst_ictx->snap_lock);
        src_size = m_src_ictx->get_image_size(m_src_ictx->snap_id);
        dst_size = m_dst_ictx->get_image_size(m_dst_ictx->snap_id);
      }
      EXPECT_EQ(src_size, dst_size);

      ssize_t read_size = 1 << m_src_ictx->order;
      uint64_t offset = 0;
      while (offset < src_size) {
        read_size = std::min(read_size, static_cast<ssize_t>(src_size - offset));

        bufferptr src_ptr(read_size);
        bufferlist src_bl;
        src_bl.push_back(src_ptr);
        librbd::io::ReadResult src_result{&src_bl};
        EXPECT_EQ(read_size, m_src_ictx->io_work_queue->read(
                    offset, read_size, librbd::io::ReadResult{src_result}, 0));

        bufferptr dst_ptr(read_size);
        bufferlist dst_bl;
        dst_bl.push_back(dst_ptr);
        librbd::io::ReadResult dst_result{&dst_bl};
        EXPECT_EQ(read_size, m_dst_ictx->io_work_queue->read(
                    offset, read_size, librbd::io::ReadResult{dst_result}, 0));

        if (!src_bl.contents_equal(dst_bl)) {
          std::cout << "snap: " << (src_snap_name ? src_snap_name : "null")
                    << ", block " << offset << "~" << read_size << " differs"
                    << std::endl;
          // std::cout << "src block: " << std::endl; src_bl.hexdump(std::cout);
          // std::cout << "dst block: " << std::endl; dst_bl.hexdump(std::cout);
        }
        EXPECT_TRUE(src_bl.contents_equal(dst_bl));
        offset += read_size;
      }
    }
  }

  void test_no_snaps() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(0 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(2 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
  }

  void test_snaps() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(0 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
    ASSERT_EQ(0, m_src_ictx->io_work_queue->flush());

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));

    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(1 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->write(0 * bl.length(), bl1.length(),
                                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->discard(bl1.length() + 10,
                                                 bl1.length(), false));

    ASSERT_EQ(0, m_src_ictx->io_work_queue->flush());

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->write(1 * bl.length(), bl1.length(),
                                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->discard(2 * bl1.length() + 10,
                                                 bl1.length(), false));
  }

  void test_snap_discard() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(0, bl.length(), bufferlist{bl},
                                               0));
    ASSERT_EQ(0, m_src_ictx->io_work_queue->flush());

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));

    size_t len = (1 << m_src_ictx->order) * 2;
    ASSERT_EQ(static_cast<ssize_t>(len),
              m_src_ictx->io_work_queue->discard(0, len, false));
  }

  void test_clone() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(0 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(2 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
    ASSERT_EQ(0, m_src_ictx->io_work_queue->flush());

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->write(0 * bl.length(), bl1.length(),
                                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->discard(bl1.length() + 10,
                                                 bl1.length(), false));
    ASSERT_EQ(0, m_src_ictx->io_work_queue->flush());

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    clone_name = get_temp_image_name();
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->write(1 * bl.length(), bl1.length(),
                                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->discard(2 * bl1.length() + 10,
                                                 bl1.length(), false));
  }

  void test_stress() {
    uint64_t initial_size, size;
    {
      RWLock::RLocker src_locker(m_src_ictx->snap_lock);
      size = initial_size = m_src_ictx->get_image_size(CEPH_NOSNAP);
    }

    int nsnaps = 4;
    const char *c = getenv("TEST_RBD_DEEPCOPY_STRESS_NSNAPS");
    if (c != NULL) {
      std::stringstream ss(c);
      ASSERT_TRUE(ss >> nsnaps);
    }

    int nwrites = 4;
    c = getenv("TEST_RBD_DEEPCOPY_STRESS_NWRITES");
    if (c != NULL) {
      std::stringstream ss(c);
      ASSERT_TRUE(ss >> nwrites);
    }

    for (int i = 0; i < nsnaps; i++) {
      for (int j = 0; j < nwrites; j++) {
        size_t len = rand() % ((1 << m_src_ictx->order) * 2);
        ASSERT_GT(size, len);
        bufferlist bl;
        bl.append(std::string(len, static_cast<char>('A' + i)));
        uint64_t off = std::min(static_cast<uint64_t>(rand() % size),
                                static_cast<uint64_t>(size - len));
        std::cout << "write: " << static_cast<char>('A' + i) << " " << off
                  << "~" << len << std::endl;
        ASSERT_EQ(static_cast<ssize_t>(bl.length()),
                  m_src_ictx->io_work_queue->write(off, bl.length(),
                                                   bufferlist{bl}, 0));
        len = rand() % ((1 << m_src_ictx->order) * 2);
        ASSERT_GT(size, len);
        off = std::min(static_cast<uint64_t>(rand() % size),
                       static_cast<uint64_t>(size - len));
        std::cout << "discard: " << off << "~" << len << std::endl;
        ASSERT_EQ(static_cast<ssize_t>(len),
                  m_src_ictx->io_work_queue->discard(off, len, false));
      }

      ASSERT_EQ(0, m_src_ictx->io_work_queue->flush());

      std::string snap_name = "snap" + stringify(i);
      std::cout << "snap: " << snap_name << std::endl;
      ASSERT_EQ(0, snap_create(*m_src_ictx, snap_name.c_str()));

      if (rand() % 2) {
        std::string clone_name = get_temp_image_name();
        int order = m_src_ictx->order;
        uint64_t features;
        ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
        std::cout << "clone " << m_src_ictx->name << " -> " << clone_name
                  << std::endl;
        ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(),
                                   snap_name.c_str(), m_ioctx,
                                   clone_name.c_str(), features, &order, 0, 0));
        close_image(m_src_ictx);
        ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));
      }

      if (rand() % 2) {
        librbd::NoOpProgressContext no_op;
        uint64_t new_size =  initial_size + rand() % size;
        std::cout << "resize: " << new_size << std::endl;
        ASSERT_EQ(0, m_src_ictx->operations->resize(new_size, true, no_op));
        {
          RWLock::RLocker src_locker(m_src_ictx->snap_lock);
          size = m_src_ictx->get_image_size(CEPH_NOSNAP);
        }
        ASSERT_EQ(new_size, size);
      }
    }
  }

  librbd::ImageCtx *m_src_ictx = nullptr;
  librbd::ImageCtx *m_dst_ictx = nullptr;
  librbd::ImageOptions m_opts;
};

TEST_F(TestDeepCopy, Empty)
{
}

TEST_F(TestDeepCopy, NoSnaps)
{
  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps)
{
  test_snaps();
}

TEST_F(TestDeepCopy, SnapDiscard)
{
  test_snap_discard();
}

TEST_F(TestDeepCopy, Clone)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone();
}

TEST_F(TestDeepCopy, Stress)
{
  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_LargerDstObjSize)
{
  uint64_t order = m_src_ictx->order + 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_LargerDstObjSize)
{
  uint64_t order = m_src_ictx->order + 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_LargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_LargerDstObjSize)
{
  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_SmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_stress();
}
