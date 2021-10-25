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


class SESQA(Task):

    def __init__(self, ctx, config):
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
        self.scripts = Scripts(self.master_remote, ses_qa_ctx['logger_obj'])
        self.sm = ses_qa_ctx['salt_manager_instance']

    def _populate_ses_qa_context(self):
        ses_qa_ctx['roles'] = self.ctx.config['roles']
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


class MGRPlugin(SESQA):

    err_prefix = "(mgr_plugin subtask) "

    def __init__(self, ctx, config):
        ses_qa_ctx['logger_obj'] = log.getChild('mgr_plugin')
        self.name = 'ses_qa.mgr_plugin'
        super(MGRPlugin, self).__init__(ctx, config)
        self.log.debug("munged config is {}".format(self.config))

    def mgr_plugin_dashboard(self, **kwargs):
        """
        Minimal/smoke test for the MGR dashboard plugin
        """
        self.scripts.mgr_dashboard_module_smoke()

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
            self.scripts.mgr_plugin_influx()
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
                "Running MGR plugin test {} with config ->{}<-"
                .format(method_spec, kwargs)
                )
            method = getattr(self, method_spec, None)
            if method:
                method(**kwargs)
            else:
                raise ConfigError(self.err_prefix + "No such method ->{}<-"
                                  .format(method_spec))


task = SESQA
mgr_plugin = MGRPlugin
