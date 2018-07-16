#!/bin/sh -xe

N=3
MAX_IN_FLIGHT_APPENDS="0 1"
MAX_PAYLOAD_BYTES="16384 262144 1048576"
FLUSH_INTERVALS="0"
FLUSH_AGES="0"
IO_SIZES="4096 65536 1048576"
IMAGE=test$$
SIZE=2G
RUNTIME=60

WDIR=$(pwd)
TEMPDIR=`mktemp -d`

echo "building fio"

cd $TEMPDIR
wget https://github.com/axboe/fio/archive/fio-2.21.tar.gz
tar -xvf fio*tar.gz
cd fio-fio*
if [ -d ${WDIR}/lib -a -d ${WDIR}/../src/include ]; then
    echo "running from build dir"
    cp -R ${WDIR}/../src/include .
    rm -f include/assert.h
    export LD_LIBRARY_PATH=${WDIR}/lib
    ./configure --extra-cflags="-I./include -L${WDIR}/lib"
else
    if sudo which apt-get; then
        DEBIAN_FRONTEND=noninteractive sudo -E apt-get -y --force-yes install librbd-dev
    else
        sudo yum install -y librbd1-dev
    fi
    ./configure
fi
make
cd ${WDIR}

rbd create -s ${SIZE} ${IMAGE}

cleanup() {
    set +e

    rm -Rf ${TEMPDIR}
    rbd rm ${IMAGE} && return
    sleep 35
    rbd rm ${IMAGE}
}

trap cleanup INT TERM EXIT

echo prefilling image ${IMAGE}

rbd bench ${IMAGE} --io-type write --io-size 4M --io-total ${SIZE}
rbd bench ${IMAGE} --io-type write --io-size 4K --io-pattern rand --io-total 20M
rbd bench ${IMAGE} --io-type write --io-size 64K --io-pattern rand --io-total 80M
rbd info ${IMAGE} | grep 'features: .*exclusive-lock' ||
    rbd feature enable ${IMAGE} exclusive-lock
rbd info ${IMAGE} | grep 'features: .*journaling' ||
    rbd feature enable ${IMAGE} journaling
rbd bench ${IMAGE} --io-type write --io-size 4K --io-pattern rand --io-total 20M

echo running benchmark

for n in `seq $N`; do
    for rbd_cache in false true; do
        echo rbd_cache: $rbd_cache
        for io_size in ${IO_SIZES}; do
            for m in $MAX_IN_FLIGHT_APPENDS; do
                echo rbd_journal_object_max_in_flight_appends: $m
                for b in $MAX_PAYLOAD_BYTES; do
                    echo rbd_journal_max_payload_bytes: $b
                    for i in $FLUSH_INTERVALS; do
                        echo rbd_journal_object_flush_interval: $i
                        for a in $FLUSH_AGES; do
                            echo rbd_journal_object_flush_age: $a
                            CEPH_ARGS="--rbd-cache=${rbd_cache} \
                                       --rbd_journal_object_max_in_flight_appends=$m \
                                       --rbd_journal_max_payload_bytes=$b \
                                       --rbd_journal_object_flush_interval=$i \
                                       --rbd_journal_object_flush_age=$a" \
                            $TEMPDIR/fio-fio-2.21/fio --ioengine=rbd \
                                --bs=${io_size} --iodepth=8 --size=80% --time_based \
                                --runtime=${RUNTIME} --clientname=admin --pool=rbd \
                                --name test --rw=randwrite --rbdname=${IMAGE}
                        done
                    done
                done
            done
        done
    done
done

echo OK
