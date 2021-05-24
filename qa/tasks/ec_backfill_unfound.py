"""
Backfill_unfound
"""
import logging
import time
from tasks import ceph_manager
from tasks.util.rados import rados
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test handling of unfound objects during backfill on an ec pool.

    A pretty rigid cluster is brought up and tested by this task
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'lost_unfound task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    profile = config.get('erasure_code_profile', {
        'k': '2',
        'm': '1',
        'crush-failure-domain': 'osd'
    })
    profile_name = profile.get('name', 'backfill_unfound')
    manager.create_erasure_code_profile(profile_name, profile)
    pool = manager.create_pool_with_unique_name(
        pg_num=1,
        erasure_code_profile_name=profile_name,
        min_size=2)
    manager.raw_cluster_cmd('osd', 'pool', 'set', pool,
                            'pg_autoscale_mode', 'off')

    manager.flush_pg_stats([0, 1, 2, 3])
    manager.wait_for_clean()

    pool_id = manager.get_pool_num(pool)
    pgid = '%d.0' % pool_id
    pgs = manager.get_pg_stats()
    acting = next((pg['acting'] for pg in pgs if pg['pgid'] == pgid), None)
    log.info("acting=%s" % acting)
    assert acting
    primary = acting[0]

    # something that is always there, readable and never empty
    dummyfile = '/etc/group'

    # kludge to make sure they get a map
    rados(ctx, mon, ['-p', pool, 'put', 'dummy', dummyfile])

    manager.flush_pg_stats([0, 1])
    manager.wait_for_recovery()

    log.info("create test object")
    obj = 'test'
    rados(ctx, mon, ['-p', pool, 'put', obj, dummyfile])

    log.info("write some data")
    rados(ctx, mon, ['-p', pool, 'bench', '30', 'write', '-b', '4096',
                          '--no-cleanup'])

    log.info("remove object from all non-primary shard and start backfill")
    victims = acting[1:]

    for i in victims:
        manager.objectstore_tool(pool, options='', args='remove',
                                 object_name=obj, osd=i)

    # mark one of the osds out to trigger a rebalance/backfill
    manager.mark_out_osd(victims[0])

    # wait for everything to peer, backfill and detect unfound object
    manager.flush_pg_stats([0, 1, 2, 3])
    manager.wait_till_active()
    while True:
        manager.flush_pg_stats([0, 1, 2, 3])
        pgs = manager.get_pg_stats()
        pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
        log.info('pg=%s' % pg);
        if 'backfilling' not in pg['state'].split('+'):
            break
        time.sleep(1)

    # verify that there is unfound object
    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.info('pg=%s' % pg);
    assert pg
    assert 'backfill_unfound' in pg['state'].split('+')
    unfound = manager.get_num_unfound_objects()
    log.info("there are %d unfound objects" % unfound)
    assert unfound == 1
    m = manager.list_pg_unfound(pgid)
    log.info('list_pg_unfound=%s' % m)
    assert m['num_unfound'] == pg['stat_sum']['num_objects_unfound']

    log.info("restart non-primary osd")
    acting = pg['acting']
    victim = acting[1]
    manager.kill_osd(victim)
    manager.mark_down_osd(victim)
    manager.wait_till_active()
    manager.revive_osd(victim)
    manager.wait_till_osd_is_up(victim)

    manager.wait_till_active()
    manager.flush_pg_stats([0, 1, 2, 3])

    unfound = manager.get_num_unfound_objects()
    log.info("there are %d unfound objects" % unfound)
    assert unfound == 1
    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.info('pg=%s' % pg)
    assert pg
    assert 'backfill_unfound' in pg['state'].split('+')
    m = manager.list_pg_unfound(pgid)
    log.info('list_pg_unfound=%s' % m)
    assert m['num_unfound'] == pg['stat_sum']['num_objects_unfound']

    # mark stuff lost
    pgs = manager.get_pg_stats()
    manager.raw_cluster_cmd('pg', pgid, 'mark_unfound_lost', 'delete')

    # wait for everything to peer and be happy...
    manager.flush_pg_stats([0, 1, 2, 3])
    manager.wait_for_recovery()
