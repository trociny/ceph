#!/usr/bin/perl -w

use strict;

print "#job_id\trbd_cache\tmax_appends\tmax_payload\tflush_interval\tflush_age\tio_size\t\tbw\t\tio\n";

my $job_id = '-------';
my $rbd_cache;
my $max_appends;
my $max_payload_bytes;
my $flush_interval;
my $flush_age;
my $key;

while (<>) {
    # job_id: '2790955'
    if (/job_id: '(\d+)'/) {
        $job_id = $1;
        next;
    }
    # rbd_cache: false
    if (/rbd_cache: ([^\s]+)/) {
        $rbd_cache = $1;
        next;
    }
    # rbd_journal_object_max_in_flight_appends: 1
    if (/rbd_journal_object_max_in_flight_appends: (\d+)/) {
        $max_appends = $1;
        next;
    }
    # rbd_journal_max_payload_bytes: 16384
    if (/rbd_journal_max_payload_bytes: (\d+)/) {
        $max_payload_bytes = $1;
        next;
    }
    # rbd_journal_object_flush_interval: 1
    if (/rbd_journal_object_flush_interval: (\d+)/) {
        $flush_interval = $1;
        next;
    }
    # rbd_journal_object_flush_age: 1
    if (/rbd_journal_object_flush_age: (\d+)/) {
        $flush_age = $1;
        next;
    }
    # fio --ioengine=rbd --bs=4096 --iodepth=8 --size=80% --time_based --runtime=60 --clientname=admin --pool=rbd --name test --rw=randwrite --rbdname=test5859
    if (/--bs=(\d+)/) {
        my $io_size = $1;
        $key = "$rbd_cache\t\t$max_appends\t\t$max_payload_bytes\t\t$flush_interval\t\t$flush_age\t\t$io_size\t";
        next;
    }

    # WRITE: bw=587KiB/s (601kB/s), 587KiB/s-587KiB/s (601kB/s-601kB/s), io=34.4MiB (36.1MB), run=60017-60017msec
    if (m|WRITE:\s+bw=([^\s]+)\s.*\sio=([^\s]+)\s.*, run=6\d\d\d\d-|) {
        my $bw = $1;
        my $io = $2;
        print "$job_id\t$key\t${bw}\t${io}\n";
    }
}
