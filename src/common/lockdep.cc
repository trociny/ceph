// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2008-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "BackTrace.h"
#include "Clock.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/valgrind.h"
#include "include/types.h"
#include "lockdep.h"

#include "include/unordered_map.h"

#if defined(__FreeBSD__) && defined(__LP64__)	// On FreeBSD pthread_t is a pointer.
namespace std {
  template<>
    struct hash<pthread_t>
    {
      size_t
      operator()(pthread_t __x) const
      { return (uintptr_t)__x; }
    };
} // namespace std
#endif

/******* Constants **********/
#define lockdep_dout(v) lsubdout(g_lockdep->ceph_ctx, lockdep, v)
#define MAX_LOCKS  4096   // increase me as needed
#define BACKTRACE_SKIP 2

/******* Globals **********/
struct LockdepContext {
  LockdepContext(CephContext *ceph_ctx) : ceph_ctx(ceph_ctx)
  {
    ANNOTATE_BENIGN_RACE_SIZED(&ceph_ctx, sizeof(ceph_ctx), "lockdep cct");
    ANNOTATE_BENIGN_RACE_SIZED(this, sizeof(*this), "lockdep enabled");
    memset(follows, 0, sizeof(follows[0][0]) * MAX_LOCKS * MAX_LOCKS);
    memset(follows_bt, 0, sizeof(follows_bt[0][0]) * MAX_LOCKS * MAX_LOCKS);

    for (int i=0; i<MAX_LOCKS; ++i) {
      free_ids.push_back(i);
    }
  }

  CephContext *ceph_ctx;
  ceph::unordered_map<std::string, int> lock_ids;
  map<int, std::string> lock_names;
  map<int, int> lock_refs;
  list<int> free_ids;
  ceph::unordered_map<pthread_t, map<int,BackTrace*> > held;
  bool follows[MAX_LOCKS][MAX_LOCKS]; // follows[a][b] means b taken after a
  BackTrace *follows_bt[MAX_LOCKS][MAX_LOCKS];
};

struct LockdepContext *g_lockdep = nullptr;

pthread_mutex_t g_lockdep_mutex = PTHREAD_MUTEX_INITIALIZER;

struct lockdep_stopper_t {
  // disable lockdep when this module destructs.
  ~lockdep_stopper_t() {
    pthread_mutex_lock(&g_lockdep_mutex);
    if (g_lockdep) {
      delete g_lockdep;
      g_lockdep = nullptr;
    }
    pthread_mutex_unlock(&g_lockdep_mutex);
  }
} lockdep_stopper;

static bool lockdep_force_backtrace()
{
  return g_lockdep != nullptr &&
    g_lockdep->ceph_ctx->_conf->lockdep_force_backtrace;
}

/******* Functions **********/
void lockdep_register_ceph_context(CephContext *cct)
{
  pthread_mutex_lock(&g_lockdep_mutex);
  if (g_lockdep == nullptr) {
    g_lockdep = new LockdepContext(cct);
  }
  lockdep_dout(0) << "lockdep start" << dendl;
  pthread_mutex_unlock(&g_lockdep_mutex);
}

void lockdep_unregister_ceph_context(CephContext *cct)
{
  pthread_mutex_lock(&g_lockdep_mutex);
  if (g_lockdep == nullptr || cct != g_lockdep->ceph_ctx) {
    return;
  }
  // this cct is going away; shut it down!
  lockdep_dout(0) << "lockdep stop" << dendl;
  delete g_lockdep;
  g_lockdep = nullptr;
  pthread_mutex_unlock(&g_lockdep_mutex);
}

int lockdep_dump_locks()
{
  pthread_mutex_lock(&g_lockdep_mutex);

  if (g_lockdep == nullptr) {
    return 0;
  }

  for (ceph::unordered_map<pthread_t, map<int,BackTrace*> >::iterator p =
	 g_lockdep->held.begin(); p != g_lockdep->held.end(); ++p) {
    lockdep_dout(0) << "--- thread " << p->first << " ---" << dendl;
    for (map<int,BackTrace*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      lockdep_dout(0) << "  * " << g_lockdep->lock_names[q->first] << "\n";
      if (q->second)
	q->second->print(*_dout);
      *_dout << dendl;
    }
  }

  pthread_mutex_unlock(&g_lockdep_mutex);
  return 0;
}


int lockdep_register(const char *name)
{
  int id;

  pthread_mutex_lock(&g_lockdep_mutex);

  if (g_lockdep == nullptr) {
    return -1;
  }

  ceph::unordered_map<std::string, int>::iterator p =
    g_lockdep->lock_ids.find(name);
  if (p == g_lockdep->lock_ids.end()) {
    if (g_lockdep->free_ids.empty()) {
      lockdep_dout(0) << "ERROR OUT OF IDS .. have "
		      << g_lockdep->free_ids.size()
		      << " max " << MAX_LOCKS << dendl;
      for (auto& p : g_lockdep->lock_names) {
	lockdep_dout(0) << "  lock " << p.first << " " << p.second << dendl;
      }
      assert(g_lockdep->free_ids.empty());
    }
    id = g_lockdep->free_ids.front();
    g_lockdep->free_ids.pop_front();

    g_lockdep->lock_ids[name] = id;
    g_lockdep->lock_names[id] = name;
    lockdep_dout(10) << "registered '" << name << "' as " << id << dendl;
  } else {
    id = p->second;
    lockdep_dout(20) << "had '" << name << "' as " << id << dendl;
  }

  ++g_lockdep->lock_refs[id];
  pthread_mutex_unlock(&g_lockdep_mutex);

  return id;
}

void lockdep_unregister(int id)
{
  if (id < 0) {
    return;
  }

  pthread_mutex_lock(&g_lockdep_mutex);

  if (g_lockdep == nullptr) {
    return;
  }

  map<int, std::string>::iterator p = g_lockdep->lock_names.find(id);
  assert(p != g_lockdep->lock_names.end());

  int &refs = g_lockdep->lock_refs[id];
  if (--refs == 0) {
    // reset dependency ordering
    for (int i=0; i<MAX_LOCKS; ++i) {
      delete g_lockdep->follows_bt[id][i];
      g_lockdep->follows_bt[id][i] = NULL;
      g_lockdep->follows[id][i] = false;

      delete g_lockdep->follows_bt[i][id];
      g_lockdep->follows_bt[i][id] = NULL;
      g_lockdep->follows[i][id] = false;
    }

    lockdep_dout(10) << "unregistered '" << p->second << "' from " << id
                     << dendl;
    g_lockdep->lock_ids.erase(p->second);
    g_lockdep->lock_names.erase(id);
    g_lockdep->lock_refs.erase(id);
    g_lockdep->free_ids.push_back(id);
  } else {
    lockdep_dout(20) << "have " << refs << " of '" << p->second << "' "
                     << "from " << id << dendl;
  }
  pthread_mutex_unlock(&g_lockdep_mutex);
}


// does b follow a?
static bool does_follow(int a, int b)
{
  if (g_lockdep->follows[a][b]) {
    lockdep_dout(0) << "\n";
    *_dout << "------------------------------------" << "\n";
    *_dout << "existing dependency " << g_lockdep->lock_names[a] << " (" << a << ") -> "
           << g_lockdep->lock_names[b] << " (" << b << ") at:\n";
    if (g_lockdep->follows_bt[a][b]) {
      g_lockdep->follows_bt[a][b]->print(*_dout);
    }
    *_dout << dendl;
    return true;
  }

  for (int i=0; i<MAX_LOCKS; i++) {
    if (g_lockdep->follows[a][i] &&
	does_follow(i, b)) {
      lockdep_dout(0) << "existing intermediate dependency " << g_lockdep->lock_names[a]
          << " (" << a << ") -> " << g_lockdep->lock_names[i] << " (" << i << ") at:\n";
      if (g_lockdep->follows_bt[a][i]) {
        g_lockdep->follows_bt[a][i]->print(*_dout);
      }
      *_dout << dendl;
      return true;
    }
  }

  return false;
}

int lockdep_will_lock(const char *name, int id, bool force_backtrace)
{
  pthread_t p = pthread_self();
  if (id < 0) id = lockdep_register(name);

  pthread_mutex_lock(&g_lockdep_mutex);

  if (g_lockdep == nullptr) {
    return id;
  }

  lockdep_dout(20) << "_will_lock " << name << " (" << id << ")" << dendl;

  // check dependency graph
  map<int, BackTrace *> &m = g_lockdep->held[p];
  for (map<int, BackTrace *>::iterator p = m.begin();
       p != m.end();
       ++p) {
    if (p->first == id) {
      lockdep_dout(0) << "\n";
      *_dout << "recursive lock of " << name << " (" << id << ")\n";
      BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
      bt->print(*_dout);
      if (p->second) {
	*_dout << "\npreviously locked at\n";
	p->second->print(*_dout);
      }
      delete bt;
      *_dout << dendl;
      assert(0);
    }
    else if (!g_lockdep->follows[p->first][id]) {
      // new dependency

      // did we just create a cycle?
      if (does_follow(id, p->first)) {
        BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
	lockdep_dout(0) << "new dependency " << g_lockdep->lock_names[p->first]
		<< " (" << p->first << ") -> " << name << " (" << id << ")"
		<< " creates a cycle at\n";
	bt->print(*_dout);
	*_dout << dendl;

	lockdep_dout(0) << "btw, i am holding these locks:" << dendl;
	for (map<int, BackTrace *>::iterator q = m.begin();
	     q != m.end();
	     ++q) {
	  lockdep_dout(0) << "  " << g_lockdep->lock_names[q->first] << " (" << q->first << ")" << dendl;
	  if (q->second) {
	    lockdep_dout(0) << " ";
	    q->second->print(*_dout);
	    *_dout << dendl;
	  }
	}

	lockdep_dout(0) << "\n" << dendl;

	// don't add this dependency, or we'll get aMutex. cycle in the graph, and
	// does_follow() won't terminate.

	assert(0);  // actually, we should just die here.
      } else {
        BackTrace *bt = NULL;
        if (force_backtrace || lockdep_force_backtrace()) {
          bt = new BackTrace(BACKTRACE_SKIP);
        }
        g_lockdep->follows[p->first][id] = true;
        g_lockdep->follows_bt[p->first][id] = bt;
	lockdep_dout(10) << g_lockdep->lock_names[p->first] << " -> " << name << " at" << dendl;
	//bt->print(*_dout);
      }
    }
  }

  pthread_mutex_unlock(&g_lockdep_mutex);
  return id;
}

int lockdep_locked(const char *name, int id, bool force_backtrace)
{
  pthread_t p = pthread_self();

  if (id < 0) id = lockdep_register(name);

  pthread_mutex_lock(&g_lockdep_mutex);

  if (g_lockdep == nullptr) {
    return id;
  }

  lockdep_dout(20) << "_locked " << name << dendl;
  if (force_backtrace || lockdep_force_backtrace())
    g_lockdep->held[p][id] = new BackTrace(BACKTRACE_SKIP);
  else
    g_lockdep->held[p][id] = 0;
  pthread_mutex_unlock(&g_lockdep_mutex);
  return id;
}

int lockdep_will_unlock(const char *name, int id)
{
  pthread_t p = pthread_self();

  if (id < 0) {
    //id = lockdep_register(name);
    assert(id == -1);
    return id;
  }

  pthread_mutex_lock(&g_lockdep_mutex);

  if (g_lockdep == nullptr) {
    return id;
  }

  lockdep_dout(20) << "_will_unlock " << name << dendl;

  // don't assert.. lockdep may be enabled at any point in time
  //assert(held.count(p));
  //assert(held[p].count(id));

  delete g_lockdep->held[p][id];
  g_lockdep->held[p].erase(id);
  pthread_mutex_unlock(&g_lockdep_mutex);
  return id;
}


