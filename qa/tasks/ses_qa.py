"""
Task (and subtasks) for SES test automation

Linter:
    flake8 --max-line-length=100
"""
import logging

from salt_manager import SaltManager
from scripts import Scripts

from teuthology.exceptions import (
    ConfigError,
    )
from teuthology.task import Task

log = logging.getLogger(__name__)
ses_qa_ctx = {}
number_of_osds_in_cluster = """sudo ceph osd tree -f json-pretty |
                               jq '[.nodes[] | select(.type == \"osd\")] | length'"""


class SESQA(Task):

    def __init__(self, ctx, config):
        global ses_qa_ctx
        super(SESQA, self).__init__(ctx, config)
        if ses_qa_ctx:
            self.log = ses_qa_ctx['logger_obj']
            self.log.debug("ses_qa_ctx already populated (we are in a subtask)")
        if not ses_qa_ctx:
            ses_qa_ctx['logger_obj'] = log
            self.log = log
            self.log.debug("populating ses_qa_ctx (we are *not* in a subtask)")
            self._populate_ses_qa_context()
        self.master_remote = ses_qa_ctx['master_remote']
        self.nodes = self.ctx['nodes']
        self.nodes_client_only = self.ctx['nodes_client_only']
        self.nodes_cluster = self.ctx['nodes_cluster']
        self.nodes_gateway = self.ctx['nodes_gateway']
        self.nodes_storage = self.ctx['nodes_storage']
        self.nodes_storage_only = self.ctx['nodes_storage_only']
        self.remote_lookup_table = self.ctx['remote_lookup_table']
        self.remotes = self.ctx['remotes']
        self.roles = self.ctx['roles']
        self.role_lookup_table = self.ctx['role_lookup_table']
        self.role_types = self.ctx['role_types']
        self.scripts = Scripts(self.ctx, self.log)
        self.sm = ses_qa_ctx['salt_manager_instance']

    def _populate_ses_qa_context(self):
        global ses_qa_ctx
        ses_qa_ctx['salt_manager_instance'] = SaltManager(self.ctx)
        ses_qa_ctx['master_remote'] = ses_qa_ctx['salt_manager_instance'].master_remote

    def os_type_and_version(self):
        os_type = self.ctx.config.get('os_type', 'unknown')
        os_version = float(self.ctx.config.get('os_version', 0))
        return (os_type, os_version)

    def setup(self):
        super(SESQA, self).setup()

    def begin(self):
        super(SESQA, self).begin()

    def end(self):
        super(SESQA, self).end()

    def teardown(self):
        super(SESQA, self).teardown()


class Validation(SESQA):

    err_prefix = "(validation subtask) "

    def __init__(self, ctx, config):
        global ses_qa_ctx
        ses_qa_ctx['logger_obj'] = log.getChild('validation')
        self.name = 'ses_qa.validation'
        super(Validation, self).__init__(ctx, config)
        self.log.debug("munged config is {}".format(self.config))

    def mgr_plugin_influx(self, **kwargs):
        """
        Minimal/smoke test for the MGR influx plugin

        Tests the 'influx' MGR plugin, but only on openSUSE Leap 15.0.

        Testing on SLE-15 is not currently possible because the influxdb
        package is not built in IBS for anything higher than SLE-12-SP4.
        Getting it to build for SLE-15 requires a newer golang stack than what
        is available in SLE-15 - see
        https://build.suse.de/project/show/NON_Public:infrastructure:icinga2
        for how another team is building it (and no, we don't want to do that).

        Testing on openSUSE Leap 15.0 is only possible because we are building
        the influxdb package in filesystems:ceph:nautilus with modified project
        metadata.

        (This problem will hopefully go away when we switch to SLE-15-SP1.)
        """
        zypper_cmd = ("sudo zypper --non-interactive --no-gpg-check "
                      "install --force --no-recommends {}")
        os_type, os_version = self.os_type_and_version()
        if os_type == 'opensuse' and os_version >= 15:
            self.ctx.cluster.run(
                args=zypper_cmd.format(' '.join(["python3-influxdb", "influxdb"]))
                )
            self.scripts.run(
                self.master_remote,
                'mgr_plugin_influx.sh',
                )
        else:
            self.log.warning(
                "mgr_plugin_influx test case not implemented for OS ->{}<-"
                .format(os_type + " " + str(os_version))
                )

    def begin(self):
        self.log.debug("Processing tests: ->{}<-".format(self.config.keys()))
        for method_spec, kwargs in self.config.iteritems():
            kwargs = {} if not kwargs else kwargs
            if not isinstance(kwargs, dict):
                raise ConfigError(self.err_prefix + "Method config must be a dict")
            self.log.info(
                "Running test {} with config ->{}<-"
                .format(method_spec, kwargs)
                )
            method = getattr(self, method_spec, None)
            if method:
                method(**kwargs)
            else:
                raise ConfigError(self.err_prefix + "No such method ->{}<-"
                                  .format(method_spec))

    def drive_replace_initiate(self, **kwargs):
        """
        Initiate Deepsea drive replacement

        Assumes there is 1 drive not being deployed (1node5disks - with DriveGroup `limit: 4`)

        In order to "hide" an existing disk from the ceph.c_v in teuthology
        the disk is formatted and mounted.
        """
        # :TODO: make this random and find correct disk device
        osd_id = "2"
        total_osds = self.master_remote.sh(number_of_osds_in_cluster)
        assert int(total_osds) == 4, "Unexpected number of osds {} (expected 4)".format(total_osds)
        self.master_remote.sh("sudo ceph osd tree --format json | tee before.json")
        self.master_remote.sh("sudo salt-run osd.replace {} 2>/dev/null".format(osd_id))
        self.master_remote.sh("sudo mkfs.ext4 -F /dev/vdd; sudo mount /dev/vdd /mnt")
        self.master_remote.sh("sudo salt-run disks.c_v_commands 2>/dev/null")
        # Output is like: ceph-volume lvm batch --no-auto /dev/vdf --yes --osd-ids 2

    def drive_replace_check(self, **kwargs):
        """
        Deepsea drive replacement after check

        Replaced osd_id should be back in the osd tree once stage.3 is ran
        """
        total_osds = self.master_remote.sh(number_of_osds_in_cluster)
        assert int(total_osds) == 4, "Unexpected number of osds {} (expected 4)".format(total_osds)
        self.master_remote.sh("sudo ceph osd tree --format json | tee after.json")
        self.master_remote.sh("diff before.json after.json && echo 'Drive Replaced OK'")


task = SESQA
validation = Validation
