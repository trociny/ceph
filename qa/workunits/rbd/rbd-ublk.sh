#!/usr/bin/env bash
set -ex

# This exercises "rbd device ... --device-type ublk", which is a thin
# wrapper around ublksrv's "ublk" control binary and its "ublk.rbd" target
# (see src/tools/rbd/action/Ublk.cc). ublksrv is temporarily vendored and
# built/packaged by ceph itself (see WITH_RBD_UBLK), but the ublk_drv
# kernel module is not ceph's to provide -- like "--device-type nbd"
# depending on the nbd kernel module, it must already be loaded wherever
# this script runs.

POOL=rbd
ANOTHER_POOL=new_default_pool$$
NS=ns
IMAGE=testrbdublk$$
SIZE=64
DATA=
DEV=

_sudo()
{
    local cmd

    if [ `id -u` -eq 0 ]
    then
	"$@"
	return $?
    fi

    # Look for the command in the user path. If it fails run it as is,
    # supposing it is in sudo path.
    cmd=`which $1 2>/dev/null` || cmd=$1
    shift
    sudo -nE "${cmd}" "$@"
}

setup()
{
    local ns x

    if [ -e CMakeCache.txt ]; then
	# running under cmake build dir

	CEPH_SRC=$(readlink -f $(dirname $0)/../../../src)
	CEPH_ROOT=${PWD}
	CEPH_BIN=${CEPH_ROOT}/bin

	export LD_LIBRARY_PATH=${CEPH_ROOT}/lib:${LD_LIBRARY_PATH}
	export PYTHONPATH=${PYTHONPATH}:${CEPH_SRC}/pybind:${CEPH_ROOT}/lib/cython_modules/lib.3
	PATH=${CEPH_BIN}:${PATH}
    fi

    _sudo echo test sudo

    trap cleanup INT TERM EXIT
    TEMPDIR=`mktemp -d`
    DATA=${TEMPDIR}/data
    dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}

    rbd namespace create ${POOL}/${NS}

    for ns in '' ${NS}; do
        rbd --dest-pool ${POOL} --dest-namespace "${ns}" --no-progress import \
            ${DATA} ${IMAGE}
    done

    # create another pool
    ceph osd pool create ${ANOTHER_POOL} 8
    rbd pool init ${ANOTHER_POOL}
}

function cleanup()
{
    local ns s

    set +e

    # if a test aborted (e.g. via set -e) while DEV was still mounted --
    # possibly quiesced/frozen by a quiesce-hook test -- unmapping a still
    # mounted/frozen device can hang indefinitely; force-unmount first, the
    # same defensive step rbd-nbd.sh's cleanup() already takes.
    mount | grep -F ${TEMPDIR}/mnt && _sudo umount -f ${TEMPDIR}/mnt

    if [ -n "${DEV}" ]
    then
	_sudo rbd device --device-type ublk unmap ${DEV}
    fi

    rm -Rf ${TEMPDIR}

    for ns in '' ${NS}; do
        if rbd -p ${POOL} --namespace "${ns}" status ${IMAGE} 2>/dev/null; then
	    for s in 0.5 1 2 4 8 16 32; do
	        sleep $s
	        rbd -p ${POOL} --namespace "${ns}" status ${IMAGE} |
                    grep 'Watchers: none' && break
	    done
	    rbd -p ${POOL} --namespace "${ns}" snap purge ${IMAGE}
	    rbd -p ${POOL} --namespace "${ns}" remove ${IMAGE}
        fi
    done
    rbd namespace remove ${POOL}/${NS}

    # cleanup/reset default pool
    rbd config global rm global rbd_default_pool
    ceph osd pool delete ${ANOTHER_POOL} ${ANOTHER_POOL} --yes-i-really-really-mean-it
}

function expect_false()
{
  if "$@"; then return 1; else return 0; fi
}

# devices are always mapped via _sudo (map requires root, see the "exit
# status test" below), so they only show up in "device list" to a caller
# who can see root-owned/privileged ublk devices -- the kernel hides a
# privileged device's info from an unprivileged, non-owning caller
# entirely, rather than just restricting some fields. Listing must go
# through _sudo too, or these helpers silently see an empty list instead
# of the real one.
function get_pid()
{
    local pool=$1
    local ns=$2

    PID=$(_sudo rbd device --device-type ublk --format xml list | xmlstarlet sel -t -v \
      "//devices/device[pool='${pool}'][namespace='${ns}'][image='${IMAGE}'][device='${DEV}']/daemon_pid")
    test -n "${PID}" || return 1
    ps -p ${PID} -C ublk.rbd
}

function get_dev_id()
{
    local pool=$1
    local ns=$2

    DEVID=$(_sudo rbd device --device-type ublk --format xml list | xmlstarlet sel -t -v \
      "//devices/device[pool='${pool}'][namespace='${ns}'][image='${IMAGE}'][device='${DEV}']/id")
    test -n "${DEVID}"
}

function get_state()
{
    local id=$1

    _sudo rbd device --device-type ublk --format xml list | xmlstarlet sel -t -v \
      "//devices/device[id='${id}']/state"
}

unmap_device()
{
    local args=$1
    local pid=$2

    _sudo rbd device --device-type ublk unmap ${args}
    _sudo rbd device --device-type ublk list | expect_false grep -w "${pid}" || return 1
    ps -C ublk.rbd | expect_false grep -w "${pid}" || return 1

    # workaround possible race between unmap and following map
    sleep 0.5
}

#
# main
#

setup

# exit status test
if [ `id -u` -ne 0 ]
then
    expect_false rbd device --device-type ublk map ${IMAGE}
fi
expect_false _sudo rbd device --device-type ublk map INVALIDIMAGE

# list format test
expect_false rbd device --device-type ublk --format INVALID list
rbd device --device-type ublk --format json --pretty-format list
rbd device --device-type ublk --format xml list

# map test
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}`
get_pid ${POOL}
_sudo rbd device --device-type ublk list | grep "${IMAGE}"

# flush test
# the block layer only issues real flush requests down to the target if
# it believes there's a volatile write cache to flush (advertised via
# UBLK_ATTR_VOLATILE_CACHE, which becomes BLK_FEAT_WRITE_CACHE) --
# without it, an fsync/journal commit is satisfied locally by the kernel
# and the target's own flush call is never made at all, so librbd's own
# writeback cache could go unflushed indefinitely on a crash.
[ "`cat /sys/block/$(basename ${DEV})/queue/write_cache`" = "write back" ]

# read test
[ "`dd if=${DATA} bs=1M | md5sum`" = "`_sudo dd if=${DEV} bs=1M | md5sum`" ]

# write test
dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
_sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
[ "`dd if=${DATA} bs=1M | md5sum`" = "`rbd -p ${POOL} --no-progress export ${IMAGE} - | md5sum`" ]
unmap_device ${DEV} ${PID}

# notrim test
DEV=`_sudo rbd device --device-type ublk --options notrim map ${POOL}/${IMAGE}`
get_pid ${POOL}
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -eq "${provisioned}" ]
# should fail discard as at time of mapping notrim was used
expect_false _sudo blkdiscard ${DEV}
sync
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -eq "${provisioned}" ]
unmap_device ${DEV} ${PID}

# trim test
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}`
get_pid ${POOL}
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -eq "${provisioned}" ]
# should honor discard as at time of mapping trim was considered by default
_sudo blkdiscard ${DEV}
sync
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  xmlstarlet sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -lt "${provisioned}" ]
unmap_device ${DEV} ${PID}

# write-zeroes test
# a sub-granularity WRITE_ZEROES (blkdiscard -z) must actually zero the
# requested range: unlike a hint-only DISCARD, librbd's own
# discard-pruning behavior (rounding a range inward to
# rbd_discard_granularity_bytes and dropping whatever doesn't fit) would
# silently leave stale data in place here if WRITE_ZEROES were wired to
# the same discard call DISCARD uses -- the kernel completes the request
# either way, so this doesn't surface as an I/O error, only as wrong data
# on a later read.
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}`
get_pid ${POOL}
dd if=/dev/urandom of=${TEMPDIR}/wz_expected bs=4096 count=4
_sudo dd if=${TEMPDIR}/wz_expected of=${DEV} bs=4096 count=4 oflag=direct
_sudo blkdiscard -z -o 4096 -l 8192 ${DEV}
dd if=/dev/zero of=${TEMPDIR}/wz_expected bs=4096 seek=1 count=2 conv=notrunc
_sudo dd if=${DEV} of=${TEMPDIR}/wz_actual bs=4096 count=4 iflag=direct
cmp ${TEMPDIR}/wz_expected ${TEMPDIR}/wz_actual
unmap_device ${DEV} ${PID}

# read-only option test
DEV=`_sudo rbd device --device-type ublk map --read-only ${POOL}/${IMAGE}`
get_pid ${POOL}

_sudo dd if=${DEV} of=/dev/null bs=1M
expect_false _sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
unmap_device ${DEV} ${PID}

# exclusive option test
DEV=`_sudo rbd device --device-type ublk map --exclusive ${POOL}/${IMAGE}`
get_pid ${POOL}

_sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
expect_false timeout 10 \
	rbd bench ${IMAGE} --io-type write --io-size=1024 --io-total=1024
unmap_device ${DEV} ${PID}
DEV=
rbd bench ${IMAGE} --io-type write --io-size=1024 --io-total=1024

# unmap by image name test
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}`
get_pid ${POOL}
unmap_device ${IMAGE} ${PID}
DEV=

# map/unmap snap test
rbd snap create ${POOL}/${IMAGE}@snap
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}@snap`
get_pid ${POOL}
unmap_device "${IMAGE}@snap" ${PID}
DEV=

# map/unmap snap test with --snap-id
SNAPID=`rbd snap ls ${POOL}/${IMAGE} | awk '$2 == "snap" {print $1}'`
DEV=`_sudo rbd device --device-type ublk map --snap-id ${SNAPID} ${POOL}/${IMAGE}`
get_pid ${POOL}
unmap_device "--snap-id ${SNAPID} ${IMAGE}" ${PID}
DEV=

# map/unmap namespace test
rbd snap create ${POOL}/${NS}/${IMAGE}@snap
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${NS}/${IMAGE}@snap`
get_pid ${POOL} ${NS}
unmap_device "${POOL}/${NS}/${IMAGE}@snap" ${PID}
DEV=

# map/unmap namespace test with --snap-id
SNAPID=`rbd snap ls ${POOL}/${NS}/${IMAGE} | awk '$2 == "snap" {print $1}'`
DEV=`_sudo rbd device --device-type ublk map --snap-id ${SNAPID} ${POOL}/${NS}/${IMAGE}`
get_pid ${POOL} ${NS}
unmap_device "--snap-id ${SNAPID} ${POOL}/${NS}/${IMAGE}" ${PID}
DEV=

# map/unmap namespace using options test
DEV=`_sudo rbd device --device-type ublk map --pool ${POOL} --namespace ${NS} --image ${IMAGE}`
get_pid ${POOL} ${NS}
unmap_device "--pool ${POOL} --namespace ${NS} --image ${IMAGE}" ${PID}
DEV=`_sudo rbd device --device-type ublk map --pool ${POOL} --namespace ${NS} --image ${IMAGE} --snap snap`
get_pid ${POOL} ${NS}
unmap_device "--pool ${POOL} --namespace ${NS} --image ${IMAGE} --snap snap" ${PID}
DEV=

# unmap by image name test 2
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}`
get_pid ${POOL}
pid=$PID
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${NS}/${IMAGE}`
get_pid ${POOL} ${NS}
unmap_device ${POOL}/${NS}/${IMAGE} ${PID}
DEV=
unmap_device ${POOL}/${IMAGE} ${pid}

# map/unmap test with just image name and expect image to come from default pool
if [ "${POOL}" = "rbd" ];then
    DEV=`_sudo rbd device --device-type ublk map ${IMAGE}`
    get_pid ${POOL}
    unmap_device ${IMAGE} ${PID}
    DEV=
fi

# map/unmap test with just image name after changing default pool
rbd config global set global rbd_default_pool ${ANOTHER_POOL}
rbd create --size 10M ${IMAGE}
DEV=`_sudo rbd device --device-type ublk map ${IMAGE}`
get_pid ${ANOTHER_POOL}
unmap_device ${IMAGE} ${PID}
DEV=

# reset
rbd config global rm global rbd_default_pool

# recovery test
# ublksrv's framework deliberately ignores plain SIGTERM for backgrounded
# daemons (see sig_handler()/setup_pthread_sigmask() in
# targets/ublksrv_tgt.cpp) to avoid an accidental signal tearing down a
# live block device -- only SIGKILL or a proper "ublk del" actually stops
# it, unlike rbd-nbd where killing the daemon immediately drops the nbd
# socket. Use SIGKILL here to exercise the kernel's/ublk_drv's detection
# of an unexpectedly-dead daemon.
#
# Devices are mapped with ublk user-recovery enabled (see execute_map() in
# Ublk.cc), so a killed daemon doesn't make the device disappear the way it
# does for nbd: the kernel instead transitions it to QUIESCED and ublksrv
# keeps its target metadata around, so it stays visible in "device list"
# (rather than being silently dropped) until a new daemon reattaches via
# "device recover".
DEV=`_sudo rbd device --device-type ublk map ${POOL}/${IMAGE}`
get_pid ${POOL}
get_dev_id ${POOL}

# write a fresh, known pattern right before killing the daemon, rather than
# relying on whatever ${DATA} happens to hold at this point in the script
# (earlier tests, e.g. the trim test's blkdiscard, have since changed the
# image's actual content), so the post-recovery check below is verifying
# recovery itself, not leftover state from unrelated tests above.
dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
_sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
sync

_sudo kill -9 ${PID}
for i in `seq 10`; do
  [ "`get_state ${DEVID}`" = QUIESCED ] && break
  sleep 1
done
[ "`get_state ${DEVID}`" = QUIESCED ]

# recover is only supported for ublk
expect_false _sudo rbd device --device-type nbd recover ${DEVID}
expect_false _sudo rbd device recover ${DEVID}

_sudo rbd device --device-type ublk recover ${DEVID}
get_pid ${POOL}
[ "`get_state ${DEVID}`" = LIVE ]
[ "`dd if=${DATA} bs=1M | md5sum`" = "`_sudo dd if=${DEV} bs=1M | md5sum`" ]
unmap_device ${DEV} ${PID}
DEV=

# quiesce test
QUIESCE_HOOK=${TEMPDIR}/quiesce.sh
DEV=`_sudo rbd device --device-type ublk map --quiesce --quiesce-hook ${QUIESCE_HOOK} ${POOL}/${IMAGE}`
get_pid ${POOL}

# test it fails if the hook does not exist
test ! -e ${QUIESCE_HOOK}
expect_false rbd snap create ${POOL}/${IMAGE}@quiesce1
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct

# test the hook is executed
touch ${QUIESCE_HOOK}
chmod +x ${QUIESCE_HOOK}
cat > ${QUIESCE_HOOK} <<EOF
#!/bin/sh
echo "test the hook is executed" >&2
echo \$1 > ${TEMPDIR}/\$2
EOF
rbd snap create ${POOL}/${IMAGE}@quiesce1
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct
# the hook runs as whatever user the ublk daemon itself is running as
# (root, via the _sudo map above), so the files it just wrote may not be
# world-readable -- read them back with the same privilege rather than
# relying on any particular daemon umask policy.
test "$(_sudo cat ${TEMPDIR}/quiesce)" = ${DEV}
test "$(_sudo cat ${TEMPDIR}/unquiesce)" = ${DEV}

# test snap create fails if the hook fails
cat > ${QUIESCE_HOOK} <<EOF
#!/bin/sh
echo "test snap create fails if the hook fails" >&2
exit 22
EOF
expect_false rbd snap create ${POOL}/${IMAGE}@quiesce2
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct

# test the hook is slow
cat > ${QUIESCE_HOOK} <<EOF
#!/bin/sh
echo "test the hook is slow" >&2
sleep 7
EOF
rbd snap create ${POOL}/${IMAGE}@quiesce2
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct
unmap_device ${DEV} ${PID}

# test the rbd-nbd_quiesce hook that comes with the distribution --
# ublk.rbd defaults --rbd-quiesce-hook to this same script when --quiesce
# is given without an explicit hook (see execute_map() in Ublk.cc): its
# "<devpath> <quiesce|unquiesce>" protocol just fsfreezes/-unfreezes
# whatever is mounted on devpath, which works the same for a ublk device
# path as it does for an nbd one. The "else" branch below (no CEPH_SRC,
# i.e. running from installed packages rather than a source build, as
# on teuthology) relies on that default resolving to an installed file
# -- the "rbd-nbd" package/subpackage, specifically, since that's the
# only thing that ships rbd-nbd_quiesce -- so a teuthology suite using
# this branch needs "rbd-nbd" in its extra_packages even though this is
# an otherwise-ublk-only test (see qa/suites/rbd/device/workloads/
# rbd_ublk.yaml). Without it, every quiesce test below fails outright:
# fork+exec of the missing hook returns ENOENT, which
# rbd_run_quiesce_hook() unconditionally maps to -EIO, indistinguishable
# in the logs from a genuine notify-transport failure.
if [ -n "${CEPH_SRC}" ]; then
    QUIESCE_HOOK=${CEPH_SRC}/tools/rbd_nbd/rbd-nbd_quiesce
    DEV=`_sudo rbd device --device-type ublk map --quiesce --quiesce-hook ${QUIESCE_HOOK} \
               ${POOL}/${IMAGE}`
else
    DEV=`_sudo rbd device --device-type ublk map --quiesce ${POOL}/${IMAGE}`
fi
get_pid ${POOL}
_sudo mkfs ${DEV}
mkdir ${TEMPDIR}/mnt
_sudo mount ${DEV} ${TEMPDIR}/mnt
rbd snap create ${POOL}/${IMAGE}@quiesce3
_sudo dd if=${DATA} of=${TEMPDIR}/mnt/test bs=1M count=1 oflag=direct
_sudo umount ${TEMPDIR}/mnt
unmap_device ${DEV} ${PID}
DEV=

# attach/detach are not supported for ublk
expect_false _sudo rbd device --device-type ublk attach --device /dev/ublkb0 ${POOL}/${IMAGE}
expect_false _sudo rbd device --device-type ublk detach ${POOL}/${IMAGE}

echo OK
