#!/bin/sh -xe

N=4
MAX_IN_FLIGHT_APPENDS="0 1 8"
FLUSH_INTERVALS="0 1 2 8"
FLUSH_AGES="0 1 2 8"
             #io-size io-total
BENCH_PARAMS="4096    20M
              16384   40M
              65536   80M
              131072  160M
              262144  320M
              524288  640M"
IMAGE=test$$
SIZE=1G

rbd create -s ${SIZE} ${IMAGE}

cleanup() {
    rbd rm ${IMAGE} && return
    sleep 35
    rbd rm ${IMAGE}
}

trap cleanup INT TERM EXIT

echo PREFILL IMAGE

rbd bench ${IMAGE} --io-type write --io-size 4M --io-total ${SIZE}
rbd bench ${IMAGE} --io-type write --io-size 4K --io-pattern rand --io-total 20M
rbd bench ${IMAGE} --io-type write --io-size 64K --io-pattern rand --io-total 80M
rbd info ${IMAGE} | grep 'features: .*exclusive-lock' ||
    rbd feature enable ${IMAGE} exclusive-lock
rbd info ${IMAGE} | grep 'features: .*journaling' ||
    rbd feature enable ${IMAGE} journaling
rbd bench ${IMAGE} --io-type write --io-size 4K --io-pattern rand --io-total 20M

for n in `seq $N`; do
    printf "%s" "${BENCH_PARAMS}" |
    while read io_size io_total; do
        for m in $MAX_IN_FLIGHT_APPENDS; do
            echo rbd_journal_object_max_in_flight_appends: $m;
            for i in $FLUSH_INTERVALS; do
                echo rbd_journal_object_flush_interval: $i;
                for a in $FLUSH_AGES; do
                    echo rbd_journal_object_flush_age: $a;
                    rbd bench ${IMAGE} --rbd-cache=false \
                        --rbd_journal_object_max_in_flight_appends $m \
                        --rbd_journal_object_flush_interval $i \
                        --rbd_journal_object_flush_age $a \
                        --io-type write --io-size ${io_size} --io-total ${io_total} \
                        --io-pattern rand
                done
            done
        done
    done
done

echo OK
