
"""
osd_perf_query module
"""

from mgr_module import MgrModule


class OSDPerfQuery(MgrModule):
    COMMANDS = [
        {
            "cmd": "osd perf query add "
                   "name=query,type=CephString,req=true",
            "desc": "add osd perf query",
            "perm": "w"
        },
        {
            "cmd": "osd perf query remove "
                   "name=query_id,type=CephInt,req=true",
            "desc": "remove osd perf query",
            "perm": "w"
        },
        {
            "cmd": "rbd perf "
                   "name=pool_id,type=CephInt,req=true "
                   "name=image_id,type=CephString,req=true",
            "desc": "test rbd perf",
            "perm": "r"
        },
    ]

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "osd perf query add":
            query_id = self.add_osd_perf_query({'type' : cmd['query']})
            return 0, str(query_id), "added query " + cmd['query'] + " with id " + str(query_id)
        elif cmd['prefix'] == "osd perf query remove":
            self.remove_osd_perf_query(cmd['query_id'])
            return 0, "", "removed query with id " + str(cmd['query_id'])
        elif cmd['prefix'] == "rbd perf":
            perf = self.get_all_rbd_perf_counters(cmd['pool_id'], cmd['image_id'])
            op = self.get_rbd_perf_counter(cmd['pool_id'], cmd['image_id'], 'op')
            inb = self.get_rbd_perf_counter(cmd['pool_id'], cmd['image_id'], 'inb')
            outb = self.get_rbd_perf_counter(cmd['pool_id'], cmd['image_id'], 'outb')
            lat = self.get_rbd_perf_counter(cmd['pool_id'], cmd['image_id'], 'lat')
            msg = "perf for %d.%s: %s\nop: %s\ninb: %s\noutb: %s\nlat: %s" % (cmd['pool_id'], cmd['image_id'], perf, op, inb, outb, lat)
            return 0, "", msg
        else:
            raise NotImplementedError(cmd['prefix'])

