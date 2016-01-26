// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/JournalReplay.h"
#include "ImageReplayer.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

namespace {

int get_image_journal_id(librados::IoCtx ioctx, const string &image_name,
			 string *journal_id) {
  dout(20) << __func__ << ": image_name=" << image_name << dendl;

  librbd::ImageCtx *image_ctx = new librbd::ImageCtx(image_name, "", nullptr,
						     ioctx, true);

  int r = image_ctx->state->open();
  if (r < 0) {
    derr << "error opening image " << image_name << ": "
  	 << cpp_strerror(r) << dendl;
    return r;
  }

  librbd::image_info_t info;
  r = librbd::info(image_ctx, info, sizeof(info));

  image_ctx->state->close();

  if (r < 0) {
    derr << "error reading image " << image_name << " stat: "
  	 << cpp_strerror(r) << dendl;
    return r;
  }

  char prefix[RBD_MAX_BLOCK_NAME_SIZE + 1];
  strncpy(prefix, info.block_name_prefix, RBD_MAX_BLOCK_NAME_SIZE);
  prefix[RBD_MAX_BLOCK_NAME_SIZE] = '\0';

  *journal_id = string(prefix + strlen(RBD_DATA_PREFIX));

  dout(20) << __func__ << ": image_name=" << image_name << ", journal_id="
	   << *journal_id << dendl;

  return 0;
}

struct ReplayHandler : public ::journal::ReplayHandler {
  ImageReplayer *replayer;
  ReplayHandler(ImageReplayer *replayer) : replayer(replayer) {}

  virtual void get() {}
  virtual void put() {}

  virtual void handle_entries_available() {
    replayer->handle_replay_ready();
  }
  virtual void handle_complete(int r) {
    replayer->handle_replay_complete(r);
  }
};

struct C_ReplayCommitted : public Context {
  ::journal::Journaler *journaler;
  ::journal::ReplayEntry replay_entry;

  C_ReplayCommitted(::journal::Journaler *journaler,
		    ::journal::ReplayEntry &&replay_entry) :
    journaler(journaler), replay_entry(std::move(replay_entry)) {
  }
  virtual void finish(int r) {
    dout(20) << "C_ReplayCommitted completing: commit_tid="
	     << replay_entry.get_commit_tid() << ", r=" << r << dendl;

    journaler->committed(replay_entry);
  }
};

} // anonymous namespace

ImageReplayer::ImageReplayer(RadosRef local, RadosRef remote,
			     const string &local_pool_name,
			     const string &remote_pool_name,
			     const string &local_image_name,
			     const string &remote_image_name) :
  m_local(local),
  m_remote(remote),
  m_local_pool_name(local_pool_name),
  m_remote_pool_name(remote_pool_name),
  m_local_image_name(local_image_name),
  m_remote_image_name(remote_image_name.empty() ? local_image_name :
		                                  remote_image_name),
  m_lock("rbd::mirror::ImageReplayer " +
	 m_local_pool_name + "/" + m_local_image_name + "-" +
	 m_remote_pool_name + "/" + m_remote_image_name),
  m_local_image_ctx(nullptr),
  m_journal_replay(nullptr),
  m_remote_journaler(nullptr),
  m_replay_handler(nullptr)
{
  assert(m_local_pool_name != m_remote_pool_name ||
	 m_local_image_name != m_remote_image_name);
}

ImageReplayer::~ImageReplayer()
{
  assert(m_local_image_ctx == nullptr);
  assert(m_journal_replay == nullptr);
  assert(m_remote_journaler == nullptr);
  assert(m_replay_handler == nullptr);
}

int ImageReplayer::start()
{
  std::string remote_journal_id;
  C_SaferCond cond;
  double commit_interval;
  int r = 0;

  r = m_local->ioctx_create(m_local_pool_name.c_str(), m_local_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for local pool " << m_local_pool_name
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote->ioctx_create(m_remote_pool_name.c_str(), m_remote_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for remote pool " << m_remote_pool_name
	 << ": " << cpp_strerror(r) << dendl;
    goto fail;
  }

  r = get_image_journal_id(m_remote_ioctx, m_remote_image_name,
			   &remote_journal_id);
  if (r < 0) {
    derr << "can't get journal_id for remote image " << m_remote_image_name
	 << dendl;
    goto fail;
  }

  dout(20) << __func__ << ": remote_image_name=" << m_remote_image_name
	   << ", remote_journa_id=" << remote_journal_id << dendl;

  m_local_image_ctx = new librbd::ImageCtx(m_local_image_name, "", NULL,
					   m_local_ioctx, false);
  r = m_local_image_ctx->state->open();
  if (r < 0) {
    derr << "error opening local image " << m_local_image_name << ": "
	 << cpp_strerror(r) << dendl;
    goto fail;
  }

  m_journal_replay = new librbd::JournalReplay(*m_local_image_ctx);

  commit_interval = m_local_image_ctx->cct->_conf->rbd_journal_commit_age;
  commit_interval = 0.5;

  m_remote_journaler = new ::journal::Journaler(m_remote_ioctx,
						remote_journal_id,
						"MIRROR", commit_interval);

  // TODO: implement something like register_client_if_not_exist().
  r = m_remote_journaler->register_client("rbd mirror");
  if (r < 0 && r != -EEXIST) {
    derr << "error registering client: " << cpp_strerror(r) << dendl;
    goto fail;
  }

  m_remote_journaler->init(&cond);
  r = cond.wait();
  if (r < 0) {
    derr << "error initializing journal: " << cpp_strerror(r) << dendl;
    goto fail;
  }

  m_replay_handler = new ReplayHandler(this);

  m_remote_journaler->start_live_replay(m_replay_handler,
					1 /* TODO: make configurable? */);

  dout(20) << __func__ << ": m_remote_journaler=" << *m_remote_journaler
	   << dendl;

  return 0;

fail:
  dout(20) << __func__ << ": fail r=" << r << dendl;

  if (m_journal_replay) {
    m_journal_replay->flush();
    delete m_replay_handler;
    m_replay_handler = nullptr;
  }

  if (m_remote_journaler) {
    m_remote_journaler->stop_replay();
    m_remote_journaler->shutdown();
    delete m_remote_journaler;
    m_remote_journaler = nullptr;
  }

  if (m_replay_handler) {
    delete m_replay_handler;
    m_replay_handler = nullptr;
  }

  if (m_local_image_ctx) {
    m_local_image_ctx->state->close();
    m_local_image_ctx = nullptr;
  }

  m_local_ioctx.close();
  m_remote_ioctx.close();

  return r;
}

void ImageReplayer::stop()
{
  dout(20) << __func__ << dendl;

  m_journal_replay->flush();
  delete m_journal_replay;
  m_journal_replay = nullptr;

  m_remote_journaler->stop_replay();
  m_remote_journaler->shutdown();
  delete m_remote_journaler;
  m_remote_journaler = nullptr;

  delete m_replay_handler;
  m_replay_handler = nullptr;

  m_local_image_ctx->state->close();
  m_local_image_ctx = nullptr;

  m_local_ioctx.close();
  m_remote_ioctx.close();
}

void ImageReplayer::handle_replay_ready()
{
  dout(20) << __func__ << dendl;

  while (true) {
    ::journal::ReplayEntry replay_entry;
    std::string tag;
    if (!m_remote_journaler->try_pop_front(&replay_entry, &tag)) {
      break;
    }

    dout(20) << "processing entry tid=" << replay_entry.get_commit_tid()
	     << ", tag=" << tag << dendl;

    bufferlist data = replay_entry.get_data();
    bufferlist::iterator it = data.begin();
    int r = m_journal_replay->process(
      it, new C_ReplayCommitted(m_remote_journaler, std::move(replay_entry)));

    if (r < 0) {
      derr << "error replaying journal entry: " << cpp_strerror(r)
	   << dendl;
      // TODO:
      // m_journaler->stop_replay();
      // if (m_close_pending) {
      //   destroy_journaler(r);
      //   return;
      // }
      // recreate_journaler(r);
    }
  }

  dout(20) << __func__ << " return" << dendl;
}

void ImageReplayer::handle_replay_complete(int r) {
  dout(20) << __func__ << ": r=" << r << dendl;

  //m_remote_journaler->stop_replay();
}

} // namespace mirror
} // namespace rbd
