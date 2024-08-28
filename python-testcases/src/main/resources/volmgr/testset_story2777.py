"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     October 2014
@author:    Padraic Doyle
@summary:   Integration test for story 2777. As a LITP User I want to restore
            to a VxVM snapshot that I have already taken, so that my system is
            in a known good state.
            Agile: STORY-2777
"""
import time
from litp_generic_test import GenericTest, attr
from litp_cli_utils import CLIUtils
from redhat_cmd_utils import RHCmdUtils
from storage_utils import StorageUtils
from vcs_utils import VCSUtils
from rest_utils import RestUtils
import test_constants


class Story2777(GenericTest):
    """
    As a LITP User I want to restore to a VxVM snapshot that I have already
    taken, so that my system is in a known good state.
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story2777, self).setUp()
        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()
        self.timeout_mins = 10
        self.cli = CLIUtils()
        self.rhcmd = RHCmdUtils()
        self.storage = StorageUtils()
        self.vcs = VCSUtils()
        ms_ip = self.get_node_att(self.ms_node, 'ipv4')
        self.rest = RestUtils(ms_ip)
        self.ms_disks = [['root', '/'], ['home', '/home'], ['var', '/var']]

    def tearDown(self):
        """Runs for every test"""

        super(Story2777, self).tearDown()

    def _append_a_file(self, node, filepath, file_strings):
        """ Append a file """
        if not file_strings:
            return False

        if not self.remote_path_exists(node, filepath, su_root=True):
            self.log("error", "Cannot append file. It doesn't exist")
            return False

        cmd = ""
        for file_line in file_strings:
            cmd += "/bin/echo '{0}' >> {1};".format(file_line, filepath)

        self.run_command(node, cmd, su_root=True, default_asserts=True)

    def _timestamp_grub_backup(self, nodes):
        """ modify the grub backup files to contain a timestamp."""
        grub_bkup = test_constants.GRUB_CONFIG_FILE + ".backup"
        timestamp = (time.strftime("%Y-%m-%d_%H:%M"))
        file_tag = "#Marking this file for story 2777 on {0}".format(timestamp)
        for node in nodes:
            self._append_a_file(node, grub_bkup, [file_tag])
        return timestamp

    def _verify_grub_timestamp(self, nodes, grub_timestamp):
        """ Verify that the active grub file contains the timestamp """
        grub_file = test_constants.GRUB_CONFIG_FILE
        for node in nodes:
            out = self.get_file_contents(node, grub_file, su_root=True)
            self.assertTrue(self.is_text_in_list(grub_timestamp, out))

    def _set_lvm_snapshots(self, state):
        """
        enables/disables snapshots for lvm, by setting snap_external
        to enable set state = true
        to disable set state = false

        neccessary because of bug 11663, LVM tasks are now run before VxVM
        tasks
        """
        fss = self.get_all_volumes(self.ms_node, vol_driver='lvm')
        self.assertNotEqual([], fss)

        prop_string = ("{0}".format(not state)).lower()

        for volume in fss:
            props = 'snap_external={0}'.format(prop_string)
            out, err, rc = \
                self.execute_cli_update_cmd(self.ms_node,
                                            volume['path'],
                                            props)

            self.assertEqual([], out)
            self.assertEqual([], err)
            self.assertEqual(0, rc)

    def _get_snapshots(self, nodes, grep_args="L_"):
        """
        Description:
            Get the name of any snapshots on the specified nodes.
        Args:
            A list of nodes
        Actions:
            1. Grep for snapshots
        Results:
            A list of snapshots lists. e.g.
            [['ms1', '/dev/vg_root/L_lv_home_'],
             ['mn1', '/dev/vg_root/L_root_'],
             [etc....]]
        """
        sshots = []
        dg_names = \
            [volume['volume_group_name'] for volume
                in self.get_all_volumes(self.ms_node)]

        for dg_name in dg_names:
            cmd = self.storage.get_vxsnap_cmd(dg_name, grep_args=grep_args)
            for node in nodes:
                out, err, ret_code = self.run_command(node, cmd, su_root=True)
                self.assertEqual([], err)
                self.assertTrue(ret_code < 2)
                if out:
                    for line in out:
                        if '_snapshot' in line:
                            sshots.append(line.split(" ")[0])
        return sshots

    def _create_package_inheritance(self, node_url, package_name, package_url):
        """
        Description:
            Create package inheritance on the test node.
        Args:
            node_url (str): node url
            package_name (str): package name
            package_url (str): package software url
        Actions:
            1. Create package inheritance using CLI.
        Results:
            Path in litp tree to the created package inheritance.
        """

        # 1. Inherit package with cli
        node_package_url = node_url + "/items/{0}".format(package_name)
        self.execute_cli_inherit_cmd(self.ms_node,
                                     node_package_url,
                                     package_url)
        return node_package_url

    def _create_package(self, package_name, expected_state):
        """
        Description:
            Create test package
        Args:
            package_name (str): package name
            expected_state (bool): If True expect positive is True
                                   if False expect positive is False
        Actions:
            1. Get software items collection path
            2. Create test package
        Results:
            stdmsg, stderr
        """

        # 1. Get items path
        items = self.find(self.ms_node, "/software", "software-item", False)
        items_path = items[0]

        # 2. Create a package with cli
        package_url = items_path + "/package"
        props = "name='{0}'".format(package_name)

        self.execute_cli_create_cmd(
            self.ms_node,
            package_url,
            "package",
            props,
            expect_positive=expected_state)

        return package_url

    def _make_fs_change(self):
        """
        Description:
            Make a change on each file_system.
        Results:
            Boolean, True if snapshot restored  or False otherwise
        """
        # Get model file systems
        fsystems = self.get_all_volumes(self.ms_node, vol_driver='lvm')
        file_text = ["testset2777 marker file"]

        # Exclude non ext4 file systems and fs where snap_size is zero
        fsystems[:] = \
            [d for d in fsystems
                if (d.get('type') == 'ext4') and (d.get('snap_size') != '0')]

        timestamp = (time.strftime("%Y-%m-%d_%H:%M"))

        for file_sys_dict in fsystems:
            file_path = file_sys_dict['mount_point'] + \
                "/marker2777_{0}".format(timestamp)

            for node in self.mn_nodes:
                node_in_file = \
                    self.get_node_url_from_filename(self.ms_node, node)
                if node_in_file in file_sys_dict['path']:

                    self.assertTrue(node,
                                    self.create_file_on_node(node,
                                                             file_path,
                                                             file_text,
                                                             su_root=True))
                    break
        for _, mount_point in self.ms_disks:
            file_path = "{0}/marker2777_{1}".format(mount_point, timestamp)
            self.assertTrue(self.create_file_on_node(self.ms_node, file_path,
                            file_text, su_root=True))
        return timestamp

    def _mark_vxvm_file_system(self, vx_fss, mark_file):
        """ _mark_vxvm_file_system """
        mount_fss = self._mount_vx_file_systems(vx_fss)

        timestamp = (time.strftime("%Y-%m-%d_%H:%M:%S"))

        for filesys in vx_fss:
            node = \
                self.get_vx_disk_node(self.ms_node,
                                      disk_group=filesys["volume_group_name"])

            filepath = filesys['mount_point'] + "/{0}".format(mark_file)

            cmd = "/bin/echo '{0}' > {1}".format(timestamp, filepath)
            self.run_command(node, cmd, su_root=True, default_asserts=True)

        self._unmount_vx_file_systems(mount_fss)

        return timestamp

    def _verify_lvm_rolled_back(self, timestamp):
        """
        Description:
            Verify that a snapshot is restored
        Results:
            Boolean, True if snapshot restored  or False otherwise
        """
        # Get model file systems
        fsystems = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        # Exclude non ext4 file systems and fs where snap_size is zero
        fsystems[:] = \
            [d for d in fsystems
                if (d.get('type') == 'ext4') and (d.get('snap_size') != '0')]

        for file_sys_dict in fsystems:
            file_path = file_sys_dict['mount_point'] + \
                "/marker2777_{0}".format(timestamp)
            for node in self.mn_nodes:
                node_in_file = \
                    self.get_node_url_from_filename(self.ms_node, node)
                if node_in_file in file_sys_dict['path']:
                    self.assertFalse(
                        self.remote_path_exists(node,
                                                file_path),
                        "File system change not rolled back. {0}:/{1}"
                        .format(node, file_path))
                    break
        for _, mount_point in self.ms_disks:
            file_path = "{0}/marker2777_{1}".format(mount_point, timestamp)
            self.assertFalse(self.remote_path_exists(self.ms_node, file_path),
                             "File system change not rolled back. {0}:/{1}"
                             .format(self.ms_node, file_path))

    def _verify_fs_rolled_back(self, vx_fss, timestamp, mark_file):
        """
        Description:
            Verify that a snapshot is restored. Files exist.
        Results:
            Boolean, True if snapshot restored  or False otherwise
        """
        fss = self._mount_vx_file_systems(vx_fss)

        for filesys in vx_fss:
            disk_group = filesys['volume_group_name']
            node = \
                self.get_vx_disk_node(self.ms_node, disk_group=disk_group)
            file_path = filesys['mount_point'] + "/{0}".format(mark_file)
            self.assertTrue(self.remote_path_exists(node,
                                                    file_path),
                            "File system change not rolled back. {0}:/{1}"
                            .format(node, file_path))
            file_text = self.get_file_contents(node, file_path, su_root=True)
            self.assertTrue(self.is_text_in_list(timestamp, file_text))
            self.log("info", "{0} is in {1}".format(timestamp, str(file_text)))

        self._unmount_vx_file_systems(fss)

    def _mount_vx_file_systems(self, vx_fss):
        """ mount_vx_file_systems """

        mounted = []
        for filesys in vx_fss:

            # Get node vg is imported on
            disk_group = filesys['volume_group_name']
            node = \
                self.get_vx_disk_node(self.ms_node, disk_group=disk_group)

            # if disk is not mounted
            if not self.remote_path_exists(node,
                                           filesys['mount_point'],
                                           False):
                self.log("info", "Dsk grp {0} with mnt pnt {1} needs mounting"
                         .format(filesys['volume_group_name'],
                                 filesys['mount_point']))

                # Mount disk
                self.create_dir_on_node(node,
                                        filesys['mount_point'],
                                        su_root=True,
                                        add_to_cleanup=False)

                device = \
                    ("/dev/vx/dsk/{0}/{1}"
                        .format(filesys['volume_group_name'],
                                filesys['volume_name']))
                cmd = \
                    ("/bin/mount -t vxfs  {0} {1}"
                        .format(device, filesys['mount_point']))
                # /bin/mount -t vxfs  /dev/vx/dsk/vg_3/vol0 /data2
                out, _, _ = \
                    self.run_command(node, cmd, su_root=True,
                                     default_asserts=True)
                self.assertEqual([], out)
                mounted.append(filesys)
            else:
                self.log("info", "Dsk grp {0} with mnt pnt {1} mounted"
                         .format(filesys['volume_group_name'],
                                 filesys['mount_point']))
        return mounted

    def _unmount_vx_file_systems(self, fss):
        """ unmount_vx_file_systems """

        for filesys in fss:
            # Get node vg is imported on
            disk_group = filesys['volume_group_name']
            node = \
                self.get_vx_disk_node(self.ms_node, disk_group=disk_group)

            # Unmount directory
            cmd = "/bin/umount {0}".format(filesys['mount_point'])
            out, _, _ = \
                self.run_command(node, cmd, su_root=True,
                                 default_asserts=True)
            self.assertEqual([], out)

        # Delete mount directory
            cmd = "/bin/rmdir {0}".format(filesys['mount_point'])
            out, _, _ = \
                self.run_command(node, cmd, su_root=True,
                                 default_asserts=True)
            self.assertEqual([], out)

    def _ensure_vcs_is_running(self):
        """
        reenables VCS in case it got disabled
        """
        # VCS is down after the test, restarting
        hastatus_cmd = self.vcs.get_hastatus_sum_cmd()
        timeout = 0
        for node in self.mn_nodes:
            _, _, rc = \
                self.run_command(node, hastatus_cmd,
                                 su_root=True)
            if rc != 0:
                # VCS is down
                hastart_cmd = self.vcs.get_hastart()
                _, _, rc = \
                    self.run_command(node, hastart_cmd,
                                     su_root=True)
                self.assertEqual(0, rc)
                _, _, rc = \
                    self.run_command(node, hastatus_cmd,
                                     su_root=True)

                while rc != 0 and timeout < 60:
                    # wait till everything is online
                    _, _, rc = \
                        self.run_command(node, hastatus_cmd,
                                         su_root=True)
                    time.sleep(10)
                    timeout += 10

    @attr('all', 'revert', 'story2777', 'story2777_tc03',
          'kgb-physical')
    def test_03_p_user_created_snapshots_not_restored(self):
        """
        @tms_id: litpcds_2777_tc03
        @tms_requirements_id: LITPCDS-2777
        @tms_title: Restore snapshot
        @tms_description:
            Verifies restore snapshot positive case.
        @tms_test_steps:
            @step: Select nodes to create snapshot on.
            @result: Node that currently holds the vxvm volume has been found
            @step: Create LVM/VXVM snapshot
            @result: non litp LVM snapshot has been created
            @result: non litp Vxvm snapshot has been created
            @step: Timestamp a file on on all snapshotted volumes.
            @result: Files are timestamped
            @step: Create a snapshot of all nodes.
            @result: Snapshots are created
            @step: Overwrite timestamp files
            @result:  Files are timestamped with a new timestamp
            @step: Create/Inherit a package (telnet)
            @result: package items are available in the model
            @step: Create Plan
            @result: Plan is successfully created
            @step: Run and wait for restore_snapshot
            @result: Litp logs still present in /var/log path.
            @result: Litp logs contains info about restore_snapshot.
            @result: A plan on "Failed" state is found after 'restore_snapshot'
            @result: Snapshots are restored
            @result: The active grub file contains the timestamp
            @result: Package is not added.
            @result: Non-litp snapshots are not removed
            @result: Snapshot volumes are removed
            @result: Snapshot properties are set correctly
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        vol_name = "snappy_the_snapshot"
        mark_file = "test03.txt"

        fss = self.get_all_volumes(self.ms_node)
        dg_name = fss[0]['volume_group_name']
        try:
            self.log('info', 'Select nodes to create snapshot on.')
            #LVM

            lvm_node = self.mn_nodes[-1]
            lvm_found_node_url = \
                self.get_node_url_from_filename(self.ms_node, lvm_node)

            lvm_vg_id = \
                (x['vg_item_id'] for x in
                    self.get_all_volumes(self.ms_node, vol_driver='lvm')
                    if lvm_found_node_url in x['path']).next()
            # VXVM
            vx_node = self.get_vx_disk_node(self.ms_node, disk_group=dg_name)

            self.log('info', 'Create LVM/VXVM snapshot')

            #LVM
            args = \
                ("-L 100M -s -n {0} {1}{2}_root"
                    .format(vol_name, test_constants.LITP_SNAPSHOT_PATH,
                            lvm_vg_id))
            cmd = self.storage.get_lvcreate_cmd(args)
            out, _, _ = \
                self.run_command(lvm_node, cmd, su_root=True,
                                 default_asserts=True)
            self.assertTrue(self.is_text_in_list('created', out))

            #VXVM
            vg_id = fss[0]['volume_name']
            args = "make source={0}/newvol={1} cachesize=16M".format(vg_id,
                                                                     vol_name)
            cmd = self.storage.get_vxsnap_cmd(dg_name, args)
            self.run_command(vx_node, cmd, su_root=True, default_asserts=True)

            args2 = "syncwait {0}".format(vg_id)
            cmd = self.storage.get_vxsnap_cmd(dg_name, args2)
            self.run_command(vx_node, cmd, su_root=True, default_asserts=True)

            self.log('info',
                     'Timestamp a file on on all snapshotted volumes.')
            timestamp = self._mark_vxvm_file_system(fss, mark_file)

            log_path = test_constants.GEN_SYSTEM_LOG_PATH
            log_len = self.get_file_len(self.ms_node, log_path)

            self.log('info', 'Create a snapshot of all nodes.')
            self.execute_and_wait_createsnapshot(self.ms_node)

            self.log('info', 'Overwrite timestamp files')
            lvm_timestamp = self._make_fs_change()
            grub_timestamp = self._timestamp_grub_backup(self.mn_nodes)
            self._mark_vxvm_file_system(fss, mark_file)

            self.log('info', 'Create/Inherit a package (telnet)')
            package_url = self._create_package("telnet", True)
            found_node_url = \
                self.get_node_url_from_filename(self.ms_node,
                                                vx_node)
            self._create_package_inheritance(found_node_url, "telnet",
                                             package_url)

            self.log('info', 'Create Plan')
            self.execute_cli_createplan_cmd(self.ms_node)

            self.log('info', 'Run and wait for restore_snapshot')
            self.execute_and_wait_restore_snapshot(self.ms_node)

            self.log('info', 'Verify litp logs are present in '
                     '/var/log path.')
            self.assertTrue(self.remote_path_exists(self.ms_node, log_path,
                                                    su_root=True))
            self.log('info', 'Check litp logs contains info about '
                     'restore_snapshot.')
            log_msg = ("Restore_Snapshot Plan created")
            self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg,
                                                  log_len=log_len))

            self.log('info',
                     'Verify that a plan on "Failed" state is found after '
                     'restore_snapshot')
            self.assertEqual(test_constants.PLAN_FAILED,
                             self.rest.get_current_plan_state_rest())

            self.log('info',
                     'Verify that the snapshots are restored')
            self._verify_lvm_rolled_back(lvm_timestamp)
            self._verify_fs_rolled_back(fss, timestamp, mark_file)

            self.log('info',
                     'Verify that the active grub file contains the timestamp')
            self._verify_grub_timestamp(self.mn_nodes, grub_timestamp)

            self.log('info', 'Verify that the package is not added.')
            chk_pkg_cmd = self.rhcmd.check_pkg_installed(['telnet'])
            out, err, ret_code = self.run_command(vx_node, chk_pkg_cmd,
                                                  su_root=True)
            self.assertTrue(1, ret_code)
            self.assertEqual([], err)
            self.assertEqual([], out)

            self.log('info',
                     'Verify that the non-litp snapshots are not removed')
            sshot_list = self.get_snapshots([lvm_node], grep_args=vol_name)
            self.assertTrue(
                self.is_text_in_list(test_constants.LITP_SNAPSHOT_PATH
                                     + vol_name, sshot_list),
                "non LITP lvm snapshot has been removed")

            sshot_list = self._get_snapshots(self.mn_nodes, vol_name)
            self.assertTrue(self.is_text_in_list(vol_name,
                                                 sshot_list),
                            "non LITP vxvm snapshot has been removed")

            self.log('info',
                     'Verify that the snapshot volumes are removed')
            sshot_list = self._get_snapshots(self.mn_nodes)
            self.assertEqual([], sshot_list,
                             '\nUnwanted snapshot volumes found on '
                             'systems after "restore_snapshot"\n{0}'
                             .format('\n'.join(sshot_list)))

            self.log('info',
                     'Verify that snapshot item is in "Initial" state')
            expected_val = 'Initial'
            snapshot_state = self.get_item_state(self.ms_node,
                                                 '/snapshots/snapshot')

            self.assertEqual(expected_val, snapshot_state,
                             'Found snapshot state set to {0}. Expected {1}'
                             .format(snapshot_state, expected_val))
        finally:
            self.log('info', 'Remove the non-litp snapshots')
            args = \
                ("-f {0}{1}".format(test_constants.LITP_SNAPSHOT_PATH,
                 vol_name))
            cmd = self.storage.get_lvremove_cmd(args)
            self.run_command(lvm_node, cmd, su_root=True)

            args = "dis {0} ".format(vol_name)
            vx_node = self.get_vx_disk_node(self.ms_node, disk_group=dg_name)
            cmd = self.storage.get_vxsnap_cmd(dg_name, args)
            self.run_command(vx_node, cmd, su_root=True)

            args = " -rf rm {0}".format(vol_name)
            cmd = self.storage.get_vxedit_cmd(dg_name, args)
            self.run_command(vx_node, cmd, su_root=True)

    @attr('all', 'revert', 'story2777', 'story2777_tc07', 'kgb-physical')
    def test_07_p_restore_fails_vxsnap_doesnt_return(self):
        """
        @tms_id: litpcds_2777_tc07
        @tms_requirements_id: LITPCDS-2777
        @tms_title: Failed restore vxsnap does not return
        @tms_description:
            Verifies that if vxsnap doesn't return restore_snapshot fails
        @tms_test_steps:
            @step: Disable LVM snapshots
            @result: LVM snapshots are disabled
            @step: Remove any existing snapshot
            @result: deployment snapshots are removed
            @step: create new deployment snapshot
            @result: Deployment snapshots is created
            @step: Back up "vxsnap" binary file
            @result: vxsnap binary is backed up
            @step: Create a dummy vxsnap binary which returns after 200 secs
            @result: Vxsnaps is replaced
            @step: Run "restore_snapshot" command
            @result: Message log contains an error message
            @result: Restore snapshot plan fails.
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Disable LVM snapshots;')
        vxsnap_path = test_constants.VXSNAP_PATH
        self._set_lvm_snapshots(False)

        self.log('info',
                 'Remove any existing snapshot and '
                 'create new deployment snapshot')
        self.execute_and_wait_createsnapshot(self.ms_node)

        # Content for the dummy "vxsnap"
        file_contents = \
            ['#!/bin/bash',
             'params="$@"',
             'if [[ $params == *"restore"* ]]',
             'then',
             '  sleep 200 ',
             'else',
             '  exec /sbin/vxsnap $params >&1',
             'fi'
             ]
        try:
            for node in self.mn_nodes:

                self.log('info', 'Back up "vxsnap" binary file')
                self.backup_file(node, vxsnap_path, backup_mode_cp=False)

                self.log('info',
                         'Create a dummy "vxsnap" binary '
                         'which runs a timed loop for 200 secs')
                create_success = self.create_file_on_node(node,
                                                          vxsnap_path,
                                                          file_contents,
                                                          su_root=True)
                self.assertTrue(create_success,
                                'File "{0}" could not be created'
                                .format(vxsnap_path))

            self.log('info', 'Run restore_snapshot')
            self.execute_cli_restoresnapshot_cmd(self.ms_node)

            # Verify that the message log contains a message indicating why
            #   the snapshot failed to restore.
            log_msg = ("(Exception message: 'No answer from node")
            self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))

            # Verify that the restore snapshot plan fails.
            completed_successfully = self.wait_for_plan_state(
                self.ms_node,
                test_constants.PLAN_COMPLETE,
                self.timeout_mins
            )
            self.assertFalse(completed_successfully)
        finally:
            self._ensure_vcs_is_running()
            self._set_lvm_snapshots(True)
            # turn on puppet
            command = self.cli.get_mco_cmd("service puppet restart -y")
            self.run_command(self.ms_node, command, su_root=True)

    @attr('manual-test', 'revert', 'story2777', 'story2777_tc10')
    def test_10_p_plan_fails_san_uncontactable_interface_down(self):
        """
        @tms_id: litpcds_2777_tc10
        @tms_requirements_id: LITPCDS-2777
        @tms_title: Restore snapshot fails if SAN becomes incontactable
        @tms_description:
            This test will verify that the plan fails and an error is
            generated if the SAN becomes uncontactable. Interface down.
        @tms_test_steps:
            @step: create new deployment snapshot
            @result: Deployment snapshots is created
            @step: Run "restore_snapshot" command
            @result: Restore snapshot plan is running
            @step: After the vxvm check tasks pause the SAN
            @result: Restore snapshot plan fails
            @result: Task to restore the vxvm snapshot fails
            @result: there is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Manual
        """
