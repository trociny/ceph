// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/mirror_snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror_snapshot/UnlinkPeerRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace mirror_snapshot {

template <>
struct UnlinkPeerRequest<MockTestImageCtx> {
  uint64_t snap_id = CEPH_NOSNAP;
  std::string mirror_peer_uuid;
  Context* on_finish = nullptr;
  static UnlinkPeerRequest* s_instance;
  static UnlinkPeerRequest *create(MockTestImageCtx *image_ctx,
                                   uint64_t snap_id,
                                   const std::string &mirror_peer_uuid,
                                   Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->snap_id = snap_id;
    s_instance->mirror_peer_uuid = mirror_peer_uuid;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  UnlinkPeerRequest() {
    s_instance = this;
  }
};

UnlinkPeerRequest<MockTestImageCtx>* UnlinkPeerRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace mirror_snapshot
} // namespace librbd

// template definitions
#include "librbd/mirror_snapshot/CreatePrimaryRequest.cc"
template class librbd::mirror_snapshot::CreatePrimaryRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace mirror_snapshot {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockMirrorSnapshotCreatePrimaryRequest : public TestMockFixture {
public:
  typedef CreatePrimaryRequest<MockTestImageCtx> MockCreatePrimaryRequest;
  typedef UnlinkPeerRequest<MockTestImageCtx> MockUnlinkPeerRequest;

  uint64_t m_snap_seq = 0;

  void snap_create(MockTestImageCtx &mock_image_ctx,
                   const cls::rbd::SnapshotNamespace &ns,
                   const std::string& snap_name) {
    ASSERT_TRUE(mock_image_ctx.snap_info.insert(
                  {m_snap_seq++,
                   SnapInfo{snap_name, ns, 0, {}, 0, 0, {}}}).second);
  }

  void expect_refresh_image(MockTestImageCtx &mock_image_ctx,
                            bool refresh_required, int r) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(refresh_required));
    if (refresh_required) {
      EXPECT_CALL(*mock_image_ctx.state, refresh(_))
        .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
    }
  }

  void expect_get_mirror_image(MockTestImageCtx &mock_image_ctx,
                               const cls::rbd::MirrorImage &mirror_image,
                               int r) {
    using ceph::encode;
    bufferlist bl;
    encode(mirror_image, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get"),
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(bl)),
                      Return(r)));
  }

  void expect_get_mirror_peers(MockTestImageCtx &mock_image_ctx,
                               const std::vector<cls::rbd::MirrorPeer> &peers,
                               int r) {
    using ceph::encode;
    bufferlist bl;
    encode(peers, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_peer_list"),
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(bl)),
                      Return(r)));
  }

  void expect_create_snapshot(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_create(_, _, _))
      .WillOnce(DoAll(
                  Invoke([this, &mock_image_ctx, r](
                             const cls::rbd::SnapshotNamespace &ns,
                             const std::string& snap_name,
                             Context *on_finish) {
                           if (r != 0) {
                             return;
                           }
                           snap_create(mock_image_ctx, ns, snap_name);
                         }),
                  WithArg<2>(CompleteContext(
                               r, mock_image_ctx.image_ctx->op_work_queue))
                  ));
  }

  void expect_unlink_peer(MockTestImageCtx &mock_image_ctx,
                          MockUnlinkPeerRequest &mock_unlink_peer_request,
                          uint64_t snap_id, const std::string &peer_uuid,
                          int r) {
    EXPECT_CALL(mock_unlink_peer_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_unlink_peer_request, snap_id,
                        peer_uuid, r]() {
                         ASSERT_EQ(mock_unlink_peer_request.mirror_peer_uuid,
                                   peer_uuid);
                         ASSERT_EQ(mock_unlink_peer_request.snap_id, snap_id);
                         if (r == 0) {
                           auto it = mock_image_ctx.snap_info.find(snap_id);
                           ASSERT_NE(it, mock_image_ctx.snap_info.end());
                           auto info =
                             boost::get<cls::rbd::MirrorPrimarySnapshotNamespace>(
                               &it->second.snap_namespace);
                           ASSERT_NE(nullptr, info);
                           ASSERT_NE(0, info->mirror_peers.erase(peer_uuid));
                           if (info->mirror_peers.empty()) {
                             mock_image_ctx.snap_info.erase(it);
                           }
                         }
                         mock_image_ctx.image_ctx->op_work_queue->queue(
                           mock_unlink_peer_request.on_finish, r);
                       }));
  }
};

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "fsid"}}, 0);
  expect_create_snapshot(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, false,
                                          nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, RefreshError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, false,
                                          nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, GetMirrorImageError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, false,
                                          nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, GetMirrorPeersError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "fsid"}}, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, false,
                                          nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, CreateSnapshotError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "fsid"}}, 0);
  expect_create_snapshot(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, false,
                                          nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, ValidateErrorNonPrimaryNotCopied) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorNonPrimarySnapshotNamespace ns{"mirror_uuid", 123};
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, true, nullptr,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, ValidateErrorDemotedNonPrimary) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorNonPrimarySnapshotNamespace ns{"mirror_uuid", 123};
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, true, true, nullptr,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, ValidateDemotedPrimary) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorPrimarySnapshotNamespace ns{false, {"uuid"}};
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "fsid"}}, 0);
  expect_create_snapshot(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, true, false, nullptr,
                                          &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, SuccessUnlinkPeer) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->config.set_val("conf_rbd_mirroring_max_mirroring_snapshots", "3");

  MockTestImageCtx mock_image_ctx(*ictx);
  for (int i = 0; i < 3; i++) {
    cls::rbd::MirrorPrimarySnapshotNamespace ns{false, {"uuid"}};
    snap_create(mock_image_ctx, ns, "mirror_snap");
  }

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "fsid"}}, 0);
  expect_create_snapshot(mock_image_ctx, 0);
  MockUnlinkPeerRequest mock_unlink_peer_request;
  auto it = mock_image_ctx.snap_info.rbegin();
  auto snap_id = (++it)->first;
  expect_unlink_peer(mock_image_ctx, mock_unlink_peer_request, snap_id, "uuid",
                     0);
  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, false, false,
                                          nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace mirror_snapshot
} // namespace librbd

