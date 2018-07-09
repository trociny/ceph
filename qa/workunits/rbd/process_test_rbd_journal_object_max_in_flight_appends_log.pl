#!/usr/bin/perl -w

use strict;

my $max_appends;
my $flush_interval;
my $flush_age;
my $key;
my %elapsed;
my %iops;
my %n;

while (<>) {
    # PREFILL IMAGE
    if (/PREFILL IMAGE/) {
        undef $max_appends;
        next;
    }
    # rbd_journal_object_max_in_flight_appends: 1
    if (/rbd_journal_object_max_in_flight_appends: (\d+)/) {
        $max_appends = $1;
        next;
    }
    if (!defined $max_appends) {
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
    # bench  type write io_size 4096 io_threads 16 bytes 209715200 pattern random
    if (/bench .*type ([^\s]+).*io_size (\d+).*io_threads (\d+).*pattern ([^\s]+)/) {
        my $type = $1;
        my $io_size = $2;
        my $io_threads = $3;
        my $pattern = $4;
        $key = "$max_appends\t\t$flush_interval\t\t$flush_age\t\t$pattern\t$type\t$io_size\t$io_threads\t";
        next;
    }

    # elapsed:    13  ops:    51200  ops/sec:  3760.30  bytes/sec: 15402186.53
    if (m|elapsed:\s+(\d+)\s.*ops/sec:\s*([\d.]+)|) {
        $elapsed{$key} += $1;
        $iops{$key} += $2;
        $n{$key}++;
        next;
    }
}

print "#max_appends\tflush_interval\tflush_age\tpattern\ttype\tio_size\tio_threads\tn\telapsed\tiops\n";

open(my $fh, "| sort -n -k6 -k 1 -k2 -k3") or die "sort failed!";

for my $key (keys %iops) {
    print $fh "$key\t$n{$key}\t" . int($elapsed{$key} / $n{$key}) . "\t" . int($iops{$key} / $n{$key}) . "\n";
}

close($fh) or die "sort failed!";
