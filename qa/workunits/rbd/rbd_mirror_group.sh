#!/bin/sh -ex
#
# rbd_mirror_group.sh - test rbd-mirror daemon for snapshot-based group mirroring
#
# The scripts starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#

MIRROR_POOL_MODE=image
MIRROR_IMAGE_MODE=snapshot

. $(dirname $0)/rbd_mirror_helpers.sh

setup

testlog "TEST: create group and test replay"
start_mirrors ${CLUSTER1}
group=test
create_group_and_enable_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
test_group_present ${CLUSTER1} ${POOL} ${group} 'present'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'down+unknown'
fi

testlog "TEST: add image to group and test replay"
image=test
create_image ${CLUSTER2} ${POOL} ${image}
group_add_image ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
test_image_present ${CLUSTER1} ${POOL} ${image} 'present'
write_image ${CLUSTER2} ${POOL} ${image} 100
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} ${POOL} ${image} 'up+replaying'
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${POOL} ${image}

testlog "TEST: remove image from group and test replay"
group_remove_image ${CLUSTER2} ${POOL} ${group} ${POOL} ${image1}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 0
group_add_image ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1

testlog "TEST: stop mirror, create group, start mirror and test replay"

stop_mirrors ${CLUSTER1}
group1=test1
create_group_and_enable_mirror ${CLUSTER2} ${POOL} ${group1}
image1=test1
create_image ${CLUSTER2} ${POOL} ${image1}
group_add_image ${CLUSTER2} ${POOL} ${group1} ${POOL} ${image1}
image2=test2
create_image ${CLUSTER2} ${PARENT_POOL} ${image2}
group_add_image ${CLUSTER2} ${POOL} ${group1} ${PARENT_POOL} ${image2}
write_image ${CLUSTER2} ${PARENT_POOL} ${image2} 100
start_mirrors ${CLUSTER1}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 2
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1} ${PARENT_POOL} ${image2}
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1} 2
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1} 'up+replaying'
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2} 'up+replaying'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'down+unknown'
fi
compare_images ${POOL} ${image1}

testlog "TEST: test the first group is replaying after restart"
write_image ${CLUSTER2} ${POOL} ${image} 100
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} ${POOL} ${image} 'up+replaying'
compare_images ${POOL} ${image}

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  testlog "TEST: stop/start/restart group via admin socket"

  admin_daemons ${CLUSTER1} rbd mirror group stop ${POOL}/${group1}
  # wait_for_group_image_replay_stopped ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
  # wait_for_group_image_replay_stopped ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
  wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group1} 2
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'
  # wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1} 'up+stopped'
  # wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2} 'up+stopped'

  admin_daemons ${CLUSTER1} rbd mirror group start ${POOL}/${group1}
  wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 2
  # wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
  # wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'
  # wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1} 'up+replaying'
  # wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2} 'up+replaying'

  admin_daemons ${CLUSTER1} rbd mirror group restart ${POOL}/${group1}
  wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 2
  # wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
  # wait_for_group_image_replay_statyed ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'
  # wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1} 'up+replaying'
  # wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2} 'up+replaying'

  flush ${CLUSTER1}
  admin_daemons ${CLUSTER1} rbd mirror group status ${POOL}/${group1}
fi

testlog "TEST: test group rename"
new_name="${group}_RENAMED"
rename_group ${CLUSTER2} ${POOL} ${group} ${new_name}
# XXXMG: group rename is not supported right now
# wait_for_group_replay_started ${CLUSTER1} ${POOL} ${new_name} 1
# wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${new_name} 'up+replaying'
# admin_daemons ${CLUSTER1} rbd mirror group status ${POOL}/${new_name}
# admin_daemons ${CLUSTER1} rbd mirror group restart ${POOL}/${new_name}
# wait_for_group_replay_started ${CLUSTER1} ${POOL} ${new_name} 1
# wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${new_name} 'up+replaying'
rename_group ${CLUSTER2} ${POOL} ${new_name} ${group}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${group}

testlog "TEST: failover and failback"
start_mirrors ${CLUSTER2}

testlog " - demote and promote same cluster"
demote_group ${CLUSTER2} ${POOL} ${group1}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group1} 2

wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+unknown'
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1} 'up+unknown'
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2} 'up+unknown'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+unknown'
# wait_for_group_image_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} ${POOL} ${image1} 'up+unknown'
# wait_for_group_image_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} ${PARENT_POOL} ${image2} 'up+unknown'
promote_group ${CLUSTER2} ${POOL} ${group1}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 2
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}

write_image ${CLUSTER2} ${POOL} ${image1} 100
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1} ${POOL} ${image1}
compare_images ${POOL} ${image1}

testlog " - failover (unmodified)"
demote_group ${CLUSTER2} ${POOL} ${group}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+unknown'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+unknown'
promote_group ${CLUSTER2} ${POOL} ${group}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_started ${CLUSTER2} ${POOL} ${group} 1
# wait_for_group_image_replay_started ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
# wait_for_group_image_replay_started ${CLUSTER2} ${POOL} ${group} ${PARENT_POOL} ${image2}

testlog " - failback (unmodified)"
demote_group ${CLUSTER1} ${POOL} ${group}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_stopped ${CLUSTER2} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+unknown'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+unknown'
promote_group ${CLUSTER2} ${POOL} ${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+stopped'
compare_images ${POOL} ${image}

testlog " - failover"
demote_group ${CLUSTER2} ${POOL} ${group1}
wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group1} 2
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+unknown'
promote_group ${CLUSTER1} ${POOL} ${group1}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_started ${CLUSTER2} ${POOL} ${group1} 2
# wait_for_group_image_replay_started ${CLUSTER2} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_started ${CLUSTER2} ${POOL} ${group1} ${PARENT_POOL} ${image2}
write_image ${CLUSTER1} ${POOL} ${image1} 100
write_image ${CLUSTER1} ${PARENT_POOL} ${image2} 100
# wait_for_group_image_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
wait_for_group_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${group1} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+replaying'
compare_images ${POOL} ${image1}
compare_images ${PARENT_POOL} ${image2}

testlog " - failback"
demote_group ${CLUSTER1} ${POOL} ${group1}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_stopped ${CLUSTER2} ${POOL} ${group1} 2
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+unknown'
promote_group ${CLUSTER2} ${POOL} ${group1}
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 2
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
write_image ${CLUSTER2} ${POOL} ${image1} 100
write_image ${CLUSTER2} ${PARENT_POOL} ${image2} 100
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER1} ${POOL} ${group1} ${POOL} ${image1}
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER1} ${POOL} ${group1} ${PARENT_POOL} ${image2}
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER1} ${POOL} ${group1} 2
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+stopped'
compare_images ${POOL} ${image1}
compare_images ${PARENT_POOL} ${image2}

testlog " - force promote"
write_image ${CLUSTER2} ${POOL} ${image} 100
# wait_for_group_image_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
promote_group ${CLUSTER1} ${POOL} ${group} '--force'
# XXXMG: needs to restart the daemons to detect promote/demote
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}

wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group} 1
# wait_for_group_image_replay_stopped ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_stopped ${CLUSTER2} ${POOL} ${group} 1
# wait_for_group_image_replay_stopped ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+stopped'
write_image ${CLUSTER1} ${POOL} ${image} 100
write_image ${CLUSTER2} ${POOL} ${image} 100

group_remove_image ${CLUSTER1} ${POOL} ${group} ${POOL} ${image1}
remove_image_retry ${CLUSTER1} ${POOL} ${image}
remove_group ${CLUSTER1} ${POOL} ${group1}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}

testlog "TEST: disable mirroring / delete non-primary group"

disable_group_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'deleted'

enable_group_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'present' 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1

testlog "TEST: disable mirror while daemon is stopped"
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
disable_group_mirror ${CLUSTER2} ${POOL} ${group}
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  test_group_present ${CLUSTER1} ${POOL} ${group} 'present'
  test_image_present ${CLUSTER1} ${POOL} ${image} 'present'
fi
start_mirrors ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'deleted'

enable_group_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'present' 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1

testlog "TEST: non-default namespace group mirroring"
testlog " - replay"
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${NS1} ${group}
create_image ${CLUSTER2} ${POOL}/${NS1} ${image}
group_add_image ${CLUSTER2} ${POOL}/${NS1} ${group} ${POOL}/${NS1} ${image}

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${NS1} ${group} 1
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL}/${NS1} ${group} ${POOL}/${NS1} ${image}
write_image ${CLUSTER2} ${POOL}/${NS1} ${image} 100
write_image ${CLUSTER2} ${POOL}/${NS2} ${image} 100
wait_for_group_replay_complete ${CLUSTER1} ${POOL}/${NS1} ${group} 1
# wait_for_group_image_replay_complete ${CLUSTER1} ${POOL}/${NS1} ${group} ${POOL}/${NS1} ${image}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS1} ${group} 'up+replaying'
compare_images ${POOL}/${NS1} ${image}

create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${NS2} ${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${NS2} ${group} 0

testlog " - disable mirroring / remove group"
disable_mirror ${CLUSTER2} ${POOL}/${NS1} ${group}
remove_group ${CLUSTER2} ${POOL}/${NS2} ${group}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS1} ${image} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL}/${NS1} ${group} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL}/${NS2} ${group} 'deleted'

testlog "TEST: simple group resync"
image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
request_resync_group ${CLUSTER1} ${POOL} ${group}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${POOL} ${image}

testlog "TEST: request group resync while daemon is offline"
stop_mirrors ${CLUSTER1}
image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
request_resync_group ${CLUSTER1} ${POOL} ${group}
start_mirrors ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
# wait_for_group_image_replay_started ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${POOL} ${image}

testlog "TEST: split-brain"
promote_group ${CLUSTER1} ${POOL} ${group} --force
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} ${PARENT_POOL} ${image} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+stopped'
write_image ${CLUSTER1} ${POOL} ${image} 10

demote_group ${CLUSTER1} ${POOL} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+error' 'split-brain'
request_resync_group ${CLUSTER1} ${POOL} ${group}
# wait_for_group_image_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} ${PARENT_POOL} ${image} 'up+replaying'
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  # teuthology will trash the daemon
  testlog "TEST: no blocklists"
  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER1} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER2} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
fi