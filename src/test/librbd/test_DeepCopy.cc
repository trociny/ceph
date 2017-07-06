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

    ASSERT_EQ(0, open_image(m_image_name, &m_src_ictx));

    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, m_src_ictx->operations->resize((1 << m_src_ictx->order) * 20,
                                                true, no_op));
    if (m_src_ictx->old_format) {
      uint64_t format = 2;
      ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FORMAT, format));
    }
  }

  void TearDown() override {
    deep_copy();
    compare();
    close_image(m_src_ictx);
    close_image(m_dst_ictx);

    TestFixture::TearDown();
  }

  void deep_copy() {
    ASSERT_EQ(0, librbd::flush(m_src_ictx));
    ASSERT_EQ(0, snap_create(*m_src_ictx, "copy"));
    ASSERT_EQ(0, librbd::snap_set(m_src_ictx, cls::rbd::UserSnapshotNamespace(),
                                  "copy"));
    std::string dst_name = get_temp_image_name();
    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, librbd::api::Image<>::deep_copy(m_src_ictx, m_src_ictx->md_ctx,
                                                 dst_name.c_str(), m_opts,
                                                 no_op));
    ASSERT_EQ(0, librbd::snap_set(m_src_ictx, cls::rbd::UserSnapshotNamespace(),
                                  nullptr));
    ASSERT_EQ(0, m_src_ictx->operations->snap_remove(
                cls::rbd::UserSnapshotNamespace(), "copy"));
    ASSERT_EQ(0, open_image(dst_name, &m_dst_ictx));
    {
      RWLock::RLocker snap_locker(m_dst_ictx->snap_lock);
      ASSERT_EQ(CEPH_NOSNAP, m_dst_ictx->get_snap_id(
                  cls::rbd::UserSnapshotNamespace(), "copy"));
    }
  }

  void compare() {
    vector<librbd::snap_info_t> src_snaps, dst_snaps;

    ASSERT_EQ(m_src_ictx->size, m_dst_ictx->size);
    ASSERT_EQ(0, librbd::snap_list(m_src_ictx, src_snaps));
    ASSERT_EQ(0, librbd::snap_list(m_dst_ictx, dst_snaps));
    ASSERT_EQ(src_snaps.size(), dst_snaps.size());
    for (size_t i = 0; i <= src_snaps.size(); i++) {
      const char *src_snap_name = nullptr;
      const char *dst_snap_name = nullptr;
      if (i < src_snaps.size()) {
        ASSERT_EQ(src_snaps[i].name, dst_snaps[i].name);
        src_snap_name = src_snaps[i].name.c_str();
        dst_snap_name = dst_snaps[i].name.c_str();
      }
      ASSERT_EQ(0, librbd::snap_set(m_src_ictx, cls::rbd::UserSnapshotNamespace(),
                                    src_snap_name));
      ASSERT_EQ(0, librbd::snap_set(m_dst_ictx, cls::rbd::UserSnapshotNamespace(),
                                    dst_snap_name));

      ssize_t read_size = 1 << m_src_ictx->order;
      uint64_t offset = 0;
      while (offset < m_src_ictx->size) {
        read_size = std::min(read_size, (ssize_t)(m_src_ictx->size - offset));

        bufferptr src_ptr(read_size);
        bufferlist src_bl;
        src_bl.push_back(src_ptr);
        librbd::io::ReadResult src_result{&src_bl};
        ASSERT_EQ(read_size, m_src_ictx->io_work_queue->read(
                    offset, read_size, librbd::io::ReadResult{src_result}, 0));

        bufferptr dst_ptr(read_size);
        bufferlist dst_bl;
        dst_bl.push_back(dst_ptr);
        librbd::io::ReadResult dst_result{&dst_bl};
        ASSERT_EQ(read_size, m_dst_ictx->io_work_queue->read(
                    offset, read_size, librbd::io::ReadResult{dst_result}, 0));

        if (!src_bl.contents_equal(dst_bl)) {
          std::cout << "snap: " << (src_snap_name ? src_snap_name : "null")
                    << ", block " << offset << "~" << read_size << " differs"
                    << std::endl;
          std::cout << "src block: " << std::endl; src_bl.hexdump(std::cout);
          std::cout << "dst block: " << std::endl; dst_bl.hexdump(std::cout);
        }
        ASSERT_TRUE(src_bl.contents_equal(dst_bl));
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
    ASSERT_EQ(0, librbd::flush(m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));

    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(1 * bl.length(), bl.length(),
                                               bufferlist{bl}, 0));
    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->write(0 * bl.length(), bl1.length(),
                                               bufferlist{bl}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              m_src_ictx->io_work_queue->discard(bl1.length() + 10,
                                                 bl1.length(), false));
  }

  void test_snap_discard() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              m_src_ictx->io_work_queue->write(0, bl.length(), bufferlist{bl},
                                               0));
    ASSERT_EQ(0, librbd::flush(m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));

    size_t len = (1 << m_src_ictx->order) * 2;
    ASSERT_EQ(static_cast<ssize_t>(len),
              m_src_ictx->io_work_queue->discard(0, len, false));
  }

  void test_stress() {
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 20; j++) {
        size_t len = rand() % ((1 << m_src_ictx->order) * 2);
        ASSERT_GT(m_src_ictx->size, len);
        bufferlist bl;
        bl.append(std::string(len, static_cast<char>('A' + i)));
        uint64_t off = std::min(static_cast<uint64_t>(rand() % m_src_ictx->size),
                                static_cast<uint64_t>(m_src_ictx->size - len));
        std::cout << "write: " << static_cast<char>('A' + i) << " " << off << "~" << len << std::endl;
        ASSERT_EQ(static_cast<ssize_t>(bl.length()),
                  m_src_ictx->io_work_queue->write(off, bl.length(),
                                                   bufferlist{bl}, 0));
        len = rand() % ((1 << m_src_ictx->order) * 2);
        ASSERT_GT(m_src_ictx->size, len);
        off = std::min(static_cast<uint64_t>(rand() % m_src_ictx->size),
                       static_cast<uint64_t>(m_src_ictx->size - len));
        std::cout << "discard: " << off << "~" << len << std::endl;
        ASSERT_EQ(static_cast<ssize_t>(len),
                  m_src_ictx->io_work_queue->discard(off, len, false));
      }

      ASSERT_EQ(0, librbd::flush(m_src_ictx));

      std::string snap_name = "snap" + stringify(i);
      std::cout << "snap: " << snap_name << std::endl;
      ASSERT_EQ(0, snap_create(*m_src_ictx, snap_name.c_str()));
    }
  }

  librbd::ImageCtx *m_src_ictx;
  librbd::ImageCtx *m_dst_ictx;
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

TEST_F(TestDeepCopy, Stress_LargerDstObjSize)
{
  uint64_t order = m_src_ictx->order + 1;
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

TEST_F(TestDeepCopy, Stress_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_stress();
}
