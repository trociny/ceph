import json
import rados
import rbd
import re

from datetime import datetime, timedelta, time
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from .common import get_rbd_pools

SCHEDULE_INTERVAL = "interval"
SCHEDULE_START_TIME = "start_time"
SCHEDULE_COUNT = "count"
SCHEDULE_CFG_KEY = "schedule"
SCHEDULE_KEY_PREFIX = "schedule_"
RETENTION_POLICY_CFG_KEY = "retention_policy"
RETENTION_POLICY_KEY_PREFIX = "retention_policy_"


class LevelSpec:

    def __init__(self, name, id, pool_id, namespace, image_id=None):
        self.name = name
        self.id = id
        self.pool_id = pool_id
        self.namespace = namespace
        self.image_id = image_id

    def __eq__(self, level_spec):
        return self.id == level_spec.id

    def is_child_of(self, level_spec):
        if level_spec.is_global():
            return not self.is_global()
        if level_spec.pool_id != self.pool_id:
            return False
        if level_spec.namespace is None:
            return self.namespace is not None
        if level_spec.namespace != self.namespace:
            return False
        if level_spec.image_id is None:
            return self.image_id is not None
        return False

    def is_global(self):
        return self.pool_id is None

    def get_pool_id(self):
        return self.pool_id

    def matches(self, pool_id, namespace, image_id=None):
        if self.pool_id and self.pool_id != pool_id:
            return False
        if self.namespace and self.namespace != namespace:
            return False
        if self.image_id and self.image_id != image_id:
            return False
        return True

    def intersects(self, level_spec):
        if self.pool_id is None or level_spec.pool_id is None:
            return True
        if self.pool_id != level_spec.pool_id:
            return False
        if self.namespace is None or level_spec.namespace is None:
            return True
        if self.namespace != level_spec.namespace:
            return False
        if self.image_id is None or level_spec.image_id is None:
            return True
        if self.image_id != level_spec.image_id:
            return False
        return True

    @classmethod
    def make_global(cls):
        return LevelSpec("", "", None, None, None)

    @classmethod
    def from_pool_spec(cls, pool_id, pool_name, namespace=None):
        if namespace is None:
            id = "{}".format(pool_id)
            name = "{}/".format(pool_name)
        else:
            id = "{}/{}".format(pool_id, namespace)
            name = "{}/{}/".format(pool_name, namespace)
        return LevelSpec(name, id, str(pool_id), namespace, None)

    @classmethod
    def from_name(cls, handler, name, namespace_validator=None,
                  image_validator=None, allow_image_level=True):
        # parse names like:
        # '', 'rbd/', 'rbd/ns/', 'rbd//image', 'rbd/image', 'rbd/ns/image'
        match = re.match(r'^(?:([^/]+)/(?:(?:([^/]*)/|)(?:([^/@]+))?)?)?$',
                         name)
        if not match:
            raise ValueError("failed to parse {}".format(name))
        if match.group(3) and not allow_image_level:
            raise ValueError(
                "invalid name {}: image level is not allowed".format(name))

        id = ""
        pool_id = None
        namespace = None
        image_name = None
        image_id = None
        if match.group(1):
            pool_name = match.group(1)
            try:
                pool_id = handler.module.rados.pool_lookup(pool_name)
                if pool_id is None:
                    raise ValueError("pool {} does not exist".format(pool_name))
                if pool_id not in get_rbd_pools(handler.module):
                    raise ValueError("{} is not an RBD pool".format(pool_name))
                pool_id = str(pool_id)
                id += pool_id
                if match.group(2) is not None or match.group(3):
                    id += "/"
                    with handler.module.rados.open_ioctx(pool_name) as ioctx:
                        namespace = match.group(2) or ""
                        if namespace:
                            namespaces = rbd.RBD().namespace_list(ioctx)
                            if namespace not in namespaces:
                                raise ValueError(
                                    "namespace {} does not exist".format(
                                        namespace))
                            id += namespace
                            ioctx.set_namespace(namespace)
                            if namespace_validator:
                                namespace_validator(ioctx)
                        if match.group(3):
                            image_name = match.group(3)
                            try:
                                with rbd.Image(ioctx, image_name,
                                               read_only=True) as image:
                                    image_id = image.id()
                                    id += "/" + image_id
                                    if image_validator:
                                        image_validator(image)
                            except rbd.ImageNotFound:
                                raise ValueError("image {} does not exist".format(
                                    image_name))
                            except rbd.InvalidArgument:
                                raise ValueError(
                                    "image {} is not in snapshot mirror mode".format(
                                        image_name))

            except rados.ObjectNotFound:
                raise ValueError("pool {} does not exist".format(pool_name))

        # normalize possible input name like 'rbd//image'
        if not namespace and image_name:
            name = "{}/{}".format(pool_name, image_name)

        return LevelSpec(name, id, pool_id, namespace, image_id)

    @classmethod
    def from_id(cls, handler, id, namespace_validator=None,
                image_validator=None):
        # parse ids like:
        # '', '123', '123/', '123/ns', '123//image_id', '123/ns/image_id'
        match = re.match(r'^(?:(\d+)(?:/([^/]*)(?:/([^/@]+))?)?)?$', id)
        if not match:
            raise ValueError("failed to parse: {}".format(id))

        name = ""
        pool_id = None
        namespace = None
        image_id = None
        if match.group(1):
            pool_id = match.group(1)
            try:
                pool_name = handler.module.rados.pool_reverse_lookup(
                    int(pool_id))
                if pool_name is None:
                    raise ValueError("pool {} does not exist".format(pool_name))
                name += pool_name + "/"
                if match.group(2) is not None or match.group(3):
                    with handler.module.rados.open_ioctx(pool_name) as ioctx:
                        namespace = match.group(2) or ""
                        if namespace:
                            namespaces = rbd.RBD().namespace_list(ioctx)
                            if namespace not in namespaces:
                                raise ValueError(
                                    "namespace {} does not exist".format(
                                        namespace))
                            name += namespace + "/"
                            ioctx.set_namespace(namespace)
                            if namespace_validator:
                                namespace_validator(ioctx)
                        elif not match.group(3):
                            name += "/"
                        if match.group(3):
                            image_id = match.group(3)
                            try:
                                with rbd.Image(ioctx, image_id=image_id,
                                               read_only=True) as image:
                                    image_name = image.get_name()
                                    name += image_name
                                    if image_validator:
                                        image_validator(image)
                            except rbd.ImageNotFound:
                                raise ValueError("image {} does not exist".format(
                                    image_id))
                            except rbd.InvalidArgument:
                                raise ValueError(
                                    "image {} is not in snapshot mirror mode".format(
                                        image_id))

            except rados.ObjectNotFound:
                raise ValueError("pool {} does not exist".format(pool_id))

        return LevelSpec(name, id, pool_id, namespace, image_id)


class Interval:

    def __init__(self, interval, units):
        self.interval = interval
        self.units = units

    def __eq__(self, interval):
        if self.units == 'mo':
            return interval.units == 'mo' and self.interval == interval.interval
        else:
            return self.minutes == interval.minutes

    def __hash__(self):
        return hash(self.minutes)

    def is_floating(self):
        return self.units == 'mo'

    @property
    def minutes(self):
        if self.units == 'm':
            return self.interval
        elif self.units == 'h':
            return self.interval * 60
        elif self.units == 'd':
            return self.interval * 60 * 24
        elif self.units == 'w':
            return self.interval * 60 * 24 * 7
        else:
            return None

    def to_string(self):
        return "{}{}".format(self.interval, self.units)

    @classmethod
    def from_string(cls, interval):
        match = re.match(r'^(\d*)(mo|w|d|h|m)?$', interval)
        if not match:
            raise ValueError("Invalid interval ({})".format(interval))

        interval = match.group(1) and int(match.group(1)) or 1
        units = match.group(2) and match.group(2) or 'm'

        return Interval(interval, units)


class StartTime:

    def __init__(self, hour, minute, tzinfo):
        self.time = time(hour, minute, tzinfo=tzinfo)
        self.minutes = self.time.hour * 60 + self.time.minute
        if self.time.tzinfo:
            self.minutes += int(self.time.utcoffset().seconds / 60)

    def __eq__(self, start_time):
        return self.minutes == start_time.minutes

    def __hash__(self):
        return hash(self.minutes)

    def to_string(self):
        return self.time.isoformat()

    @classmethod
    def from_string(cls, start_time):
        if not start_time:
            return None

        try:
            t = parse(start_time).timetz()
        except ValueError as e:
            raise ValueError("Invalid start time {}: {}".format(start_time, e))

        return StartTime(t.hour, t.minute, tzinfo=t.tzinfo)


class Schedule:

    def __init__(self, name):
        self.name = name
        self.items = set()

    def __len__(self):
        return len(self.items)

    def add(self, interval, start_time=None):
        self.items.add((interval, start_time))

    def remove(self, interval, start_time=None):
        self.items.discard((interval, start_time))

    def next_run(self, now):
        schedule_time = None
        for item in self.items:
            interval = item[0]
            start_time = datetime(1970, 1, 1)
            if item[1]:
                start_time += timedelta(minutes=item[1].minutes)
            if interval.units == 'mo':
                period = interval.interval
                d = relativedelta(now, start_time)
                months=(int((d.years * 12 + d.months) / period) + 1) * period
                time = start_time + relativedelta(months=months)
            else:
                period = timedelta(minutes=interval.minutes)
                time = start_time + \
                    (int((now - start_time) / period) + 1) * period
            if schedule_time is None or time < schedule_time:
                schedule_time = time
        return datetime.strftime(schedule_time, "%Y-%m-%d %H:%M:00")

    def to_list(self):
        return [{SCHEDULE_INTERVAL: i[0].to_string(),
                 SCHEDULE_START_TIME: i[1] and i[1].to_string() or None}
                for i in self.items]

    def to_json(self):
        return json.dumps(self.to_list(), indent=4, sort_keys=True)

    @classmethod
    def from_json(cls, name, val):
        try:
            items = json.loads(val)
            schedule = Schedule(name)
            for item in items:
                interval = Interval.from_string(item[SCHEDULE_INTERVAL])
                start_time = item[SCHEDULE_START_TIME] and \
                    StartTime.from_string(item[SCHEDULE_START_TIME]) or None
                schedule.add(interval, start_time)
            return schedule
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON ({})".format(str(e)))
        except KeyError as e:
            raise ValueError(
                "Invalid schedule format (missing key {})".format(str(e)))
        except TypeError as e:
            raise ValueError("Invalid schedule format ({})".format(str(e)))

class Schedules:

    def __init__(self, handler, format=1):
        self.handler = handler
        self.format = format
        self.level_specs = {}
        self.schedules = {}

    def __len__(self):
        return len(self.schedules)

    def load(self, namespace_validator=None, image_validator=None):

        schedule_cfg = self.handler.module.get_localized_module_option(
            self.handler.MODULE_OPTION_NAME, '')
        try:
            if schedule_cfg and self.format > 1:
                global_cfg = json.loads(schedule_cfg)
                schedule_cfg = json.dumps(
                    global_cfg.get(SCHEDULE_CFG_KEY, []))
            if schedule_cfg:
                level_spec = LevelSpec.make_global()
                self.level_specs[level_spec.id] = level_spec
                schedule = Schedule.from_json(level_spec.name, schedule_cfg)
                self.schedules[level_spec.id] = schedule
        except ValueError:
            self.handler.log.error(
                "Failed to decode configured schedule {}".format(schedule_cfg))

        for pool_id, pool_name in get_rbd_pools(self.handler.module).items():
            try:
                with self.handler.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                    self.load_from_pool(ioctx, namespace_validator,
                                        image_validator)
            except rados.Error as e:
                self.handler.log.error(
                    "Failed to load schedules for pool {}: {}".format(
                        pool_name, e))

    def load_from_pool(self, ioctx, namespace_validator, image_validator):
        pool_id = ioctx.get_pool_id()
        pool_name = ioctx.get_pool_name()
        stale_keys = ()
        start_after = ''
        try:
            while True:
                with rados.ReadOpCtx() as read_op:
                    self.handler.log.info(
                        "load_schedules: {}, start_after={}".format(
                            pool_name, start_after))
                    it, ret = ioctx.get_omap_vals(read_op, start_after, "", 128)
                    ioctx.operate_read_op(read_op, self.handler.SCHEDULE_OID)

                    it = list(it)
                    for k, v in it:
                        start_after = k
                        if self.format > 1:
                            if not k.startswith(SCHEDULE_KEY_PREFIX):
                                continue
                            k = k[len(SCHEDULE_KEY_PREFIX):]
                        v = v.decode()
                        self.handler.log.info(
                            "load_schedule: {} {}".format(k, v))
                        try:
                            try:
                                level_spec = LevelSpec.from_id(
                                    self.handler, k, namespace_validator,
                                    image_validator)
                            except ValueError as e:
                                self.handler.log.debug(
                                    "Stale schedule key {} in pool {}: {}".format(
                                        k, pool_name, e))
                                if self.format == 1:
                                    stale_keys += (k,)
                                else:
                                    stale_keys += (SCHEDULE_KEY_PREFIX + k,)
                                continue

                            self.level_specs[level_spec.id] = level_spec
                            schedule = Schedule.from_json(level_spec.name, v)
                            self.schedules[level_spec.id] = schedule
                        except ValueError:
                            self.handler.log.error(
                                "Failed to decode schedule: pool={}, {} {}".format(
                                    pool_name, k, v))
                    if not it:
                        break

        except StopIteration:
            pass
        except rados.ObjectNotFound:
            pass

        if stale_keys:
            with rados.WriteOpCtx() as write_op:
                ioctx.remove_omap_keys(write_op, stale_keys)
                ioctx.operate_write_op(write_op, self.handler.SCHEDULE_OID)

    def save(self, level_spec, schedule):
        if level_spec.is_global():
            if self.format == 1:
                global_cfg_str = schedule and schedule.to_json() or ''
            else:
                global_cfg_str = self.handler.module.get_localized_module_option(
                    self.handler.MODULE_OPTION_NAME, '')
                global_cfg = global_cfg_str and json.loads(global_cfg_str) or {}
                if schedule:
                    global_cfg[SCHEDULE_CFG_KEY] = schedule.to_list()
                else:
                    global_cfg.pop(SCHEDULE_CFG_KEY, None)
                global_cfg_str = global_cfg and json.dumps(global_cfg) or ''
            self.handler.module.set_localized_module_option(
                self.handler.MODULE_OPTION_NAME, global_cfg_str)
            return

        pool_id = level_spec.get_pool_id()
        with self.handler.module.rados.open_ioctx2(int(pool_id)) as ioctx:
            with rados.WriteOpCtx() as write_op:
                if self.format == 1:
                    key = level_spec.id
                else:
                    key = SCHEDULE_KEY_PREFIX + level_spec.id
                if schedule:
                    ioctx.set_omap(write_op, (key, ), (schedule.to_json(), ))
                else:
                    ioctx.remove_omap_keys(write_op, (key, ))
                ioctx.operate_write_op(write_op, self.handler.SCHEDULE_OID)


    def add(self, level_spec, interval, start_time):
        schedule = self.schedules.get(level_spec.id, Schedule(level_spec.name))
        schedule.add(Interval.from_string(interval),
                     StartTime.from_string(start_time))
        self.schedules[level_spec.id] = schedule
        self.level_specs[level_spec.id] = level_spec
        self.save(level_spec, schedule)

    def remove(self, level_spec, interval, start_time):
        schedule = self.schedules.pop(level_spec.id, None)
        if schedule:
            if interval is None:
                schedule = None
            else:
                schedule.remove(Interval.from_string(interval),
                                StartTime.from_string(start_time))
                if schedule:
                    self.schedules[level_spec.id] = schedule
            if not schedule:
                del self.level_specs[level_spec.id]
        self.save(level_spec, schedule)

    def find(self, pool_id, namespace, image_id=None):
        levels = [None, pool_id, namespace]
        if image_id:
            levels.append(image_id)

        while levels:
            level_spec_id = "/".join(levels[1:])
            if level_spec_id in self.schedules:
                return self.schedules[level_spec_id]
            del levels[-1]
        return None

    def intersects(self, level_spec):
        for ls in self.level_specs.values():
            if ls.intersects(level_spec):
                return True
        return False

    def to_list(self, level_spec):
        if level_spec.id in self.schedules:
            parent = level_spec
        else:
            # try to find existing parent
            parent = None
            for level_spec_id in self.schedules:
                ls = self.level_specs[level_spec_id]
                if ls == level_spec:
                    parent = ls
                    break
                if level_spec.is_child_of(ls) and \
                   (not parent or ls.is_child_of(parent)):
                    parent = ls
            if not parent:
                # set to non-existing parent so we still could list its children
                parent = level_spec

        result = {}
        for level_spec_id, schedule in self.schedules.items():
            ls = self.level_specs[level_spec_id]
            if ls == parent or ls == level_spec or ls.is_child_of(level_spec):
                result[level_spec_id] = {
                    'name' : schedule.name,
                    'schedule' : schedule.to_list(),
                }
        return result


class RetentionPolicy:

    def __init__(self, name):
        self.name = name
        self.intervals = {}

    def __len__(self):
        return len(self.intervals)

    def add(self, interval, count):
        self.intervals[interval] = count

    def remove(self, interval):
        self.intervals.pop(interval, None)

    def next_run(self, now):
        schedule_time = None
        for interval in self.intervals:
            start_time = datetime(1970, 1, 1)
            if interval.units == 'mo':
                period = interval.interval
                d = relativedelta(now, start_time)
                months=(int((d.years * 12 + d.months) / period) + 1) * period
                time = start_time + relativedelta(months=months)
            else:
                period = timedelta(minutes=interval.minutes)
                time = start_time + \
                    (int((now - start_time) / period) + 1) * period
            if schedule_time is None or time < schedule_time:
                schedule_time = time
        return datetime.strftime(schedule_time, "%Y-%m-%d %H:%M:00")

    def apply(self, now, items, log):
        log.debug("XXXMG: now={} items={}".format(now, items))
        if not items:
            return []

        class TimePoint:
            def __init__(self, name):
                self.name = name
                self.intervals = set()

        min_time = items[min(items, key=items.get)]
        time_points = {time : TimePoint(name) for name, time in items.items()}
        now = datetime(now.year, now.month, now.day, now.hour, now.minute)
        log.debug("XXXMG: now={} min_time={} time_points".format(now, min_time, time_points))

        for interval, count in self.intervals.items():
            log.debug("XXXMG: interval={} count={}".format(interval.to_string(), count))
            if not count:
                count = 2**31
            if interval.units == 'mo':
                log.debug("XXXMG: interval={}: skip for now".format(interval.to_string()))
                continue
            for i in range(count):
                time = now - timedelta(minutes=interval.minutes * i)
                if time < min_time - timedelta(minutes=interval.minutes):
                    log.debug("XXXMG: time={} => break".format(time))
                    break
                log.debug("XXXMG: add interval={} at {}".format(interval.to_string(), time))
                tp = time_points.pop(time, TimePoint(None))
                tp.intervals.add(interval)
                time_points[time] = tp

        to_keep = set()
        times = sorted(time_points)

        def find_nearest(start_time, max_distance_minutes, i):
            max_distance = timedelta(minutes=max_distance_minutes)
            while i < len(times):
                time = times[i]
                if abs(time - start_time) > max_distance:
                    return None
                tp = time_points[time]
                if tp.name:
                    return tp.name
                i += 1
            return None

        for i in range(len(times)):
            time = times[i]
            tp = time_points[time]
            log.debug("XXXMG: time={} tp={}".format(time, tp.name))
            for interval in tp.intervals:
                log.debug("XXXMG: interval={}".format(interval.to_string()))
                if interval.units == 'mo':
                    log.debug("XXXMG: interval={}: skip for now".format(interval.to_string()))
                    continue
                name = find_nearest(time, interval.minutes, i)
                log.debug("XXXMG: nearest name={}".format(name))
                if name:
                    to_keep.add(name)
                    break
                log.debug("XXXMG: closest item not found")

        log.debug("XXXMG: to_keep={}".format(to_keep))

        return [name for name in sorted(items) if name not in to_keep]

    def to_list(self):
        return [{SCHEDULE_INTERVAL: i.to_string(), SCHEDULE_COUNT: c}
                for i, c in self.intervals.items()]

    def to_json(self):
        return json.dumps(self.to_list(), indent=4, sort_keys=True)

    @classmethod
    def from_list(cls, name, items):
        policy = RetentionPolicy(name)
        try:
            for item in items:
                interval = Interval.from_string(item[SCHEDULE_INTERVAL])
                count = item[SCHEDULE_COUNT]
                policy.add(interval, count)
        except KeyError as e:
            raise ValueError(
                "Invalid retention policy format (missing key {})".format(e))
        except TypeError as e:
            raise ValueError("Invalid retention policy format ({})".format(e))
        return policy

    @classmethod
    def from_json(cls, name, val):
        try:
            items = json.loads(val)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON ({})".format(e))
        return RetentionPolicy.from_list(name, items)


class RetentionPolicies:

    def __init__(self, handler):
        self.handler = handler
        self.level_specs = {}
        self.policies = {}

    def __len__(self):
        return len(self.policies)

    def load(self, namespace_validator=None, image_validator=None):

        global_cfg_str = self.handler.module.get_localized_module_option(
            self.handler.MODULE_OPTION_NAME, '')
        if global_cfg_str:
            try:
                global_cfg = json.loads(global_cfg_str)
                items = global_cfg.get(RETENTION_POLICY_CFG_KEY)
                if items:
                    level_spec = LevelSpec.make_global()
                    self.level_specs[level_spec.id] = level_spec
                    policy = RetentionPolicy.from_list(level_spec.name, items)
                    self.policies[level_spec.id] = policy
            except ValueError:
                self.handler.log.error(
                    "Failed to decode global config {}".format(global_cfg_str))

        for pool_id, pool_name in get_rbd_pools(self.handler.module).items():
            try:
                with self.handler.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                    self.load_from_pool(ioctx, namespace_validator,
                                        image_validator)
            except rados.Error as e:
                self.handler.log.error(
                    "Failed to load retention policies for pool {}: {}".format(
                        pool_name, e))

    def load_from_pool(self, ioctx, namespace_validator, image_validator):
        pool_id = ioctx.get_pool_id()
        pool_name = ioctx.get_pool_name()
        stale_keys = ()
        start_after = ''
        try:
            while True:
                with rados.ReadOpCtx() as read_op:
                    self.handler.log.info(
                        "load retention policies: {}, start_after={}".format(
                            pool_name, start_after))
                    it, ret = ioctx.get_omap_vals(read_op, start_after, "", 128)
                    ioctx.operate_read_op(read_op, self.handler.SCHEDULE_OID)

                    it = list(it)
                    for k, v in it:
                        start_after = k
                        if not k.startswith(RETENTION_POLICY_KEY_PREFIX):
                            continue
                        k = k[len(RETENTION_POLICY_KEY_PREFIX):]
                        v = v.decode()
                        self.handler.log.info(
                            "load retention policy: {} {}".format(k, v))
                        try:
                            try:
                                level_spec = LevelSpec.from_id(
                                    self.handler, k, namespace_validator,
                                    image_validator)
                            except ValueError as e:
                                self.handler.log.debug(
                                    "Stale policy key {} in pool {}: {}".format(
                                        k, pool_name, e))
                                stale_keys += (RETENTION_POLICY_KEY_PREFIX + k,)
                                continue

                            policy = RetentionPolicy.from_json(level_spec.name, v)
                            self.level_specs[level_spec.id] = level_spec
                            self.policies[level_spec.id] = policy
                        except ValueError:
                            self.handler.log.error(
                                "Failed to decode retention policy: pool={}, {} {}".format(
                                    pool_name, k, v))
                    if not it:
                        break

        except StopIteration:
            pass
        except rados.ObjectNotFound:
            pass

        if stale_keys:
            with rados.WriteOpCtx() as write_op:
                ioctx.remove_omap_keys(write_op, stale_keys)
                ioctx.operate_write_op(write_op, self.handler.SCHEDULE_OID)

    def save(self, level_spec, policy):
        if level_spec.is_global():
            global_cfg_str = self.handler.module.get_localized_module_option(
                self.handler.MODULE_OPTION_NAME, '')
            global_cfg = global_cfg_str and json.loads(global_cfg_str) or {}
            if policy:
                global_cfg[RETENTION_POLICY_CFG_KEY] = policy.to_list()
            else:
                global_cfg.pop(RETENTION_POLICY_CFG_KEY, None)
            global_cfg_str = global_cfg and json.dumps(global_cfg) or ''
            self.handler.module.set_localized_module_option(
                self.handler.MODULE_OPTION_NAME, global_cfg_str)
            return

        pool_id = level_spec.get_pool_id()
        with self.handler.module.rados.open_ioctx2(int(pool_id)) as ioctx:
            with rados.WriteOpCtx() as write_op:
                key = RETENTION_POLICY_KEY_PREFIX + level_spec.id
                if policy:
                    ioctx.set_omap(write_op, (key, ), (policy.to_json(), ))
                else:
                    ioctx.remove_omap_keys(write_op, (key, ))
                ioctx.operate_write_op(write_op, self.handler.SCHEDULE_OID)

    def add(self, level_spec, interval, count):
        policy = self.policies.get(level_spec.id,
                                   RetentionPolicy(level_spec.name))
        policy.add(Interval.from_string(interval), count)
        self.policies[level_spec.id] = policy
        self.level_specs[level_spec.id] = level_spec
        self.save(level_spec, policy)

    def remove(self, level_spec, interval):
        policy = self.policies.pop(level_spec.id, None)
        if policy:
            if interval is None:
                policy = None
            else:
                policy.remove(Interval.from_string(interval))
                if policy:
                    self.policies[level_spec.id] = policy
            if not policy:
                del self.level_specs[level_spec.id]
        self.save(level_spec, policy)

    def find(self, pool_id, namespace, image_id=None):
        levels = [None, pool_id, namespace]
        if image_id:
            levels.append(image_id)

        while levels:
            level_spec_id = "/".join(levels[1:])
            if level_spec_id in self.policies:
                return self.policies[level_spec_id]
            del levels[-1]
        return None

    def intersects(self, level_spec):
        for ls in self.level_specs.values():
            if ls.intersects(level_spec):
                return True
        return False

    def to_list(self, level_spec):
        if level_spec.id in self.policies:
            parent = level_spec
        else:
            # try to find existing parent
            parent = None
            for level_spec_id in self.policies:
                ls = self.level_specs[level_spec_id]
                if ls == level_spec:
                    parent = ls
                    break
                if level_spec.is_child_of(ls) and \
                   (not parent or ls.is_child_of(parent)):
                    parent = ls
            if not parent:
                # set to non-existing parent so we still could list its children
                parent = level_spec

        result = {}
        for level_spec_id, policy in self.policies.items():
            ls = self.level_specs[level_spec_id]
            if ls == parent or ls == level_spec or ls.is_child_of(level_spec):
                result[level_spec_id] = {
                    'name' : policy.name,
                    'retention_policy' : policy.to_list(),
                }
        return result
