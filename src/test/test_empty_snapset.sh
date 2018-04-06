#!/bin/sh -ex

SRC_DIR=../src

ceph osd pool delete rbd rbd --yes-i-really-really-mean-it || :
ceph osd pool create rbd 32
rbd pool init

rbd create test -s 100M
image_id=$(rbd info test | awk '$1 == "id:" {print $2}')
test -n "${image_id}"

rbd bench --io-type write --io-total 20M --io-pattern rand test
rbd snap create test@snap1

rbd bench --io-type write --io-total 20M --io-pattern rand test
rbd snap create test@snap2
rbd snap create test@snap3

rbd bench --io-type write --io-total 20M --io-pattern rand test

test_snap1_md5=$(rbd export test@snap1 - | md5sum)
test_snap2_md5=$(rbd export test@snap2 - | md5sum)
test_snap3_md5=$(rbd export test@snap3 - | md5sum)
test_md5=$(rbd export test - | md5sum)

ID=2

osd_cmd=$(ps auxww | sed -ne 's/^.* \([^ ]*[c]eph-osd -i '${ID}'.*\)$/\1/p')
test -n "${osd_cmd}"
pid=$(ps auxww | awk "/[c]eph-osd -i ${ID}/ {print \$2}")
test -n "${pid}"
kill "${pid}"
sleep 2

id=$(ceph-objectstore-tool --data-path ./dev/osd${ID} --op meta-list |
         grep snapmapper)
test -n "${id}"
obj_key=``

ceph-objectstore-tool --data-path ./dev/osd${ID} "${id}" list-omap |
grep "^OBJ_.*${image_id}" |
head -20 |
while read obj_key; do
    test -n "${obj_key}"
    obj_snaps=$(ceph-objectstore-tool --data-path ./dev/osd$ID "${id}" get-omap \
                                      "${obj_key}" | base64 -w0)
    test -n "${obj_snaps}"
    obj_snaps=$(echo ${obj_snaps} | ceph_test_snap_mapper_clear_snapset -d)
    test -n "${obj_snaps}"
    echo -n "${obj_snaps}" | base64 -d |
    ceph-objectstore-tool --data-path ./dev/osd$ID "${id}" set-omap "${obj_key}"
    obj_snaps_ref=$(ceph-objectstore-tool --data-path ./dev/osd$ID "${id}" \
                                          get-omap "${obj_key}" | base64 -w0)
    test "${obj_snaps}" = "${obj_snaps_ref}"
done

rm out/osd.$ID.log

${osd_cmd} --osd_debug_verify_snaps=true
sleep 5
ceph osd scrub $ID
for i in `seq 100`; do
    sleep 1
    ps auxww | grep "[c]eph-osd -i ${ID}" || break
done

ps auxww | grep "[c]eph-osd -i ${ID}" && exit 1

grep -i -A5 'assert.*osd_debug_verify_snaps' out/osd.$ID.log
rm out/osd.$ID.log

${osd_cmd}    

sleep 5

test_snap1_md5_ref=$(rbd export test@snap1 - | md5sum)
test_snap2_md5_ref=$(rbd export test@snap2 - | md5sum)
test_snap3_md5_ref=$(rbd export test@snap3 - | md5sum)
test_md5_ref=$(rbd export test - | md5sum)

test "${test_snap1_md5_ref}" = "${test_snap1_md5}"
test "${test_snap2_md5_ref}" = "${test_snap2_md5}"
test "${test_snap3_md5_ref}" = "${test_snap3_md5}"
test "${test_md5_ref}" = "${test_md5}"

rbd bench --io-type write --io-total 20M --io-pattern rand test
rbd snap create test@snap4
rbd bench --io-type write --io-total 20M --io-pattern rand test

ceph osd scrub $ID

while ! grep 'empty snapset' out/osd.$ID.log; do
    sleep 1
done

rbd snap purge test
rbd remove test

sleep 5
rados -p rbd ls | grep "${image_id}" && exit 1

grep 'empty snapset' out/osd.$ID.log

echo OK
