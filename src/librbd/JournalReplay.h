// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REPLAY_H
#define CEPH_LIBRBD_JOURNAL_REPLAY_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "librbd/JournalTypes.h"
#include <boost/variant.hpp>
#include <set>

namespace librbd {

class AioCompletion;
class ImageCtx;

class JournalReplay {
public:
  JournalReplay(ImageCtx &image_ctx);
  ~JournalReplay();

  int process(bufferlist::iterator it, uint64_t commit_tid);
  int flush();

private:
  typedef std::set<AioCompletion *> AioCompletions;

  struct EventVisitor : public boost::static_visitor<void> {
    JournalReplay *journal_replay;
    uint64_t commit_tid;

    EventVisitor(JournalReplay *_journal_replay, uint64_t _commit_tid)
      : journal_replay(_journal_replay), commit_tid(_commit_tid) {
    }

    template <typename Event>
    inline void operator()(const Event &event) const {
      journal_replay->handle_event(event, commit_tid);
    }
  };

  ImageCtx &m_image_ctx;

  Mutex m_lock;
  Cond m_cond;

  AioCompletions m_aio_completions;
  int m_ret_val;

  void handle_event(const journal::AioDiscardEvent &event, uint64_t commit_tid);
  void handle_event(const journal::AioWriteEvent &event, uint64_t commit_tid);
  void handle_event(const journal::AioFlushEvent &event, uint64_t commit_tid);
  void handle_event(const journal::OpFinishEvent &event, uint64_t commit_tid);
  void handle_event(const journal::SnapCreateEvent &event, uint64_t commit_tid);
  void handle_event(const journal::SnapRemoveEvent &event, uint64_t commit_tid);
  void handle_event(const journal::SnapProtectEvent &event, uint64_t commit_tid);
  void handle_event(const journal::SnapUnprotectEvent &event, uint64_t commit_tid);
  void handle_event(const journal::SnapRollbackEvent &event, uint64_t commit_tid);
  void handle_event(const journal::RenameEvent &event, uint64_t commit_tid);
  void handle_event(const journal::ResizeEvent &event, uint64_t commit_tid);
  void handle_event(const journal::FlattenEvent &event, uint64_t commit_tid);
  void handle_event(const journal::UnknownEvent &event, uint64_t commit_tid);

  AioCompletion *create_aio_completion(uint64_t commit_tid);
  void handle_aio_completion(AioCompletion *aio_comp);

  static void aio_completion_callback(completion_t cb, void *arg);
};

} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_REPLAY_H
