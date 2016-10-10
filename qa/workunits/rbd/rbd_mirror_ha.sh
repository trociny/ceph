#!/bin/sh
#
# rbd_mirror_ha.sh - test rbd-mirror daemons in HA mode
#

if [ -n "${CEPH_REF}" ]; then
  wget -O rbd_mirror_helpers.sh "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=qa/workunits/rbd/rbd_mirror_helpers.sh"
  . ./rbd_mirror_helpers.sh
else
  . $(dirname $0)/rbd_mirror_helpers.sh
fi

test_replay()
{
    local instance=$1 ; shift
    local image

    for image; do
	wait_for_image_replay_started ${CLUSTER1}:${instance} ${POOL} ${image}
	write_image ${CLUSTER2} ${POOL} ${image} 100
	wait_for_replay_complete ${CLUSTER1}:${instance} ${CLUSTER2} ${POOL} \
				 ${image}
	wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' \
				    'master_position'
	if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
	    wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} \
					'down+unknown'
	fi
	compare_images ${POOL} ${image}
    done
}

testlog "TEST: start the first daemon instance and test replay"
start_mirror ${CLUSTER1}:0
image1=test1
create_image ${CLUSTER2} ${POOL} ${image1}
test_replay 0 ${image1}

testlog "TEST: start the second daemon instance and test replay"
start_mirror ${CLUSTER1}:1
image2=test2
create_image ${CLUSTER2} ${POOL} ${image2}
test_replay 0 ${image1} ${image2}

testlog "TEST: stop the first daemon instance and test replay"
stop_mirror ${CLUSTER1}:0
image3=test3
create_image ${CLUSTER2} ${POOL} ${image3}
test_replay 1 ${image1} ${image2} ${image3}

testlog "TEST: start the first daemon instance and test replay"
start_mirror ${CLUSTER1}:0
image4=test4
create_image ${CLUSTER2} ${POOL} ${image4}
test_replay 1 ${image3} ${image4}

testlog "TEST: crash the first daemon instance and test replay"
stop_mirror ${CLUSTER1}:0 -KILL
image5=test5
create_image ${CLUSTER2} ${POOL} ${image5}
test_replay 1 ${image1} ${image4} ${image5}

testlog "TEST: start the first daemon instance and test replay"
start_mirror ${CLUSTER1}:0
image6=test6
create_image ${CLUSTER2} ${POOL} ${image6}
test_replay 1 ${image1} ${image2} ${image3} ${image4} ${image5} ${image6}

echo OK
