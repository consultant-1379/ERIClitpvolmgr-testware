"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     June 2014
@author:    Padraic Doyle
@summary:   LITPCDS-2482
            Integration test for story 2482. As a LITP User I want to restore
            to a LVM snapshot that I have already taken, so that my system is
            in a known good state.
            LITPCDS-10877
            This story makes "-f" redundant. "restore_snapshot" command is now
            always executed with "-f" implicitly present
"""
import time
from litp_generic_test import GenericTest, attr
from litp_cli_utils import CLIUtils
from redhat_cmd_utils import RHCmdUtils
from storage_utils import StorageUtils
import test_constants


class Story2482(GenericTest):
    """
    As a LITP User I want to restore to a LVM snapshot that I have already
    taken, so that my system is in a known good state.
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story2482, self).setUp()
        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()
        self.all_nodes = self.mn_nodes + [self.ms_node]
        self.timeout_mins = 7
        self.cli = CLIUtils()
        self.rhcmd = RHCmdUtils()
        self.storage = StorageUtils()

    def tearDown(self):
        """Runs for every test"""
        super(Story2482, self).tearDown()

    def _get_snapshot_file_systems(self):
        """
            Return all file systems which should be snapshotted
        """
        # Get model file systems
        fsystems = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        # Exclude non xfs file systems and fs where snap_size is zero
        fsystems[:] = [d for d in fsystems if ((d.get('type') == 'xfs') and
                                                 (d.get('snap_size') != '0'))]

        # Add the un-modeled MS file systems /root /home /var
        ms_filename = self.ms_node

        ms_disks = [['root', '/'], ['home', '/home'], ['var', '/var']]

        for disk in ms_disks:
            fsystems.append({'volume_name': disk[0],
                             'mount_point': disk[1],
                             'node_name': ms_filename,
                             })

        for fsys in fsystems:
            self.log("info", "### fsys {0}".format(fsys))

        return fsystems

    def _sshot_timestamp_exists(self):
        """
        Description:
            Determine if a snapshot has a timestamp.
            A snapshot with no timestamp indicates a failed/partial snapshot.
        Results:
            Boolean, True if timestamp exists or False otherwise
        """
        # If the snapshot item exist look for timestamp
        if self.is_snapshot_item_present(self.ms_node):
            tstamp = self.get_props_from_url(self.ms_node,
                                             "/snapshots/snapshot",
                                             "timestamp")
            if tstamp:
                try:
                    float(tstamp)
                    return True
                except ValueError:
                    return False
        return False

    def _make_fs_change(self):
        """
        Description:
            Make a change on each file_system.
        Results:
            Boolean, True if snapshot restored  or False otherwise
        """
        timestamp = (time.strftime("%Y-%m-%d_%H:%M"))

        file_systems = self._get_snapshot_file_systems()

        for file_sys_dict in file_systems:
            # if file_sys_dict['type'] == 'xfs':
            file_path = file_sys_dict['mount_point'] + \
                "/marker2482_{0}".format(timestamp)
            self.assertTrue(self.create_file_on_node(
            file_sys_dict['node_name'],
            file_path,
            ["testset2482 marker file"],
            su_root=True))

        return timestamp

    def _verify_fs_rolled_back(self, timestamp):
        """
        Description:
            Verify that a snapshot is restored
        Results:
            Boolean, True if snapshot restored  or False otherwise
        """
        file_systems = self._get_snapshot_file_systems()

        for file_sys_dict in file_systems:
            file_path = file_sys_dict['mount_point'] + \
                "/marker2482_{0}".format(timestamp)
            self.assertFalse(self.remote_path_exists(
                             file_sys_dict['node_name'],
                             file_path, su_root=True),
                "File system change not rolled back. {0}:/{1}"
                    .format(file_sys_dict['node_name'], file_path)
            )

    def _verify_restore_snapshot_completes(self, timeout_mins=30):
        """
            verify restore snapshot completes
        """
        # Wait for the MS node to become unreachable
        ms_ip = self.get_node_att(self.ms_node, 'ipv4')
        self.assertTrue(self.wait_for_ping(ms_ip, False, timeout_mins,
                                           retry_count=2),
                        "Node has not gone down")

        # Wipe active SSH connections to force a reconnect
        self.disconnect_all_nodes()

        # Wait for MS to be reachable again after reboot
        self.assertTrue(self.wait_for_node_up(self.ms_node))
        # Wait for litpd service to be running
        service_command = self.rhc.get_service_running_cmd('litpd')
        self.assertTrue(self.wait_for_cmd(self.ms_node,
                                          service_command,
                                          0,
                                          timeout_mins=5),
                        "litpd service is not online")
        # Wait for nodes to be reachable
        for node in self.mn_nodes:
            self.assertTrue(self.wait_for_node_up(node))

        # Waiting for snapshot to merge
        self.log('info', 'Waiting for snapshot to merge')
        cmd = "/sbin/lvs | /bin/awk '{print $3}' | /bin/grep 'Owi'"
        for node in self.mn_nodes + [self.ms_node]:
            self.assertTrue(self.wait_for_cmd(node, cmd, 1,
                              timeout_mins=timeout_mins, su_root=True))

        self.execute_cli_showplan_cmd(self.ms_node)
        # Turn on debug
        self.turn_on_litp_debug(self.ms_node)

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

        stdout, stderr, returnc = self.run_command(node, cmd,
                                                   su_root=True)

        if returnc != 0 or stderr != [] or stdout != []:
            return False

        return True

    def _timestamp_grub_backup(self, nodes):
        """ modify the grub backup files to contain a timestamp."""
        grub_bkup = test_constants.GRUB_CONFIG_FILE + ".backup"
        timestamp = (time.strftime("%Y-%m-%d_%H:%M"))
        file_tag = "#Marking this file for story 2482 on {0}".format(timestamp)
        for node in nodes:
            appended_successfully = self._append_a_file(node,
                             grub_bkup, [file_tag])
            self.assertTrue(appended_successfully)

        return timestamp

    def _verify_grub_timestamp(self, nodes, grub_timestamp):
        """ Verify that the active grub file contains the timestamp """
        grub_file = test_constants.GRUB_CONFIG_FILE
        cmd = self.rhcmd.get_grep_file_cmd(grub_file, grub_timestamp)
        for node in nodes:
            out, err, ret_code = self.run_command(node,
                                                  cmd,
                                                  su_root=True)
            self.assertEqual(0, ret_code)
            self.assertEqual([], err)
            self.assertTrue(self.is_text_in_list(grub_timestamp, out))

    def _cleanup_after_failed_restore(self):
        """
        turns on HTTPD, Puppet
        """
        # turn on everything that was disabled during the restore plan
        # turn on puppet
        command = self.cli.get_mco_cmd("service puppet restart -y")
        self.run_command(self.ms_node, command, su_root=True)
        # turn on httpd
        self.restart_service(self.ms_node, "httpd", assert_success=False)

    def _restore_snapshot(self):
        """
        Prepares and runs a restore snapshot plan
        Verifies that remove/stop plan cannot run during restore_snapshot
        """
        self.log('info', 'Create a snapshot of all nodes.')
        if not self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_createsnapshot(self.ms_node)

        self.log('info',
            'Create a timestamped file on all snapshotted volumes.')
        file_timestamp = self._make_fs_change()

        self.log('info',
            'Modify the grub backup files to contain a timestamp.')
        grub_timestamp = self._timestamp_grub_backup(self.all_nodes)

        self.log('info', 'Run the litp restore_snapshot command')
        self.execute_cli_restoresnapshot_cmd(self.ms_node)

        self.log('info', 'Run stop_plan')
        _, err, _ = self.execute_cli_stopplan_cmd(self.ms_node,
                                                  expect_positive=False)

        self.log('info',
        'Verify that LITP replies with InvalidRequestError.')
        self.assertTrue(self.is_text_in_list("InvalidRequestError    "
                "Cannot stop plan when restore is ongoing",
                err))

        self.log('info', 'Run remove_plan')
        _, stderr, _ = self.execute_cli_removeplan_cmd(
            self.ms_node, expect_positive=False)

        self.log('info',
            'Verify that MS replies with InvalidRequestError')
        self.assertTrue(
            self.is_text_in_list(
                "InvalidRequestError    "
                "Removing a running/stopping plan is not allowed",
                stderr
            ),
            "'InvalidRequestError' relating to removing "
            "plan when running plan"
        )

        self.log('info', 'Verify that the restore snapshot completes.')
        self._verify_restore_snapshot_completes()

        self.log('info', 'Verify that the snapshots are restored')
        self._verify_fs_rolled_back(file_timestamp)

        self.log('info',
            'Verify that the active grub file contains the timestamp')
        self._verify_grub_timestamp(self.all_nodes, grub_timestamp)

        self.log('info', 'Verify that the snapshot volumes are removed')
        sshot_list = self.get_snapshots(self.all_nodes)
        self.assertEqual([], sshot_list)

    @attr('all', 'revert', 'story2482', 'story2482_tc17')
    def test_17_n_restore_snapshot_invalid_plan_fail(self):
        """
        @tms_id: litpcds_2482_tc17
        @tms_requirements_id: LITPCDS-2482, LITPCDS-10877
        @tms_title: restore snapshot fails if snapshot becomes invalid
        @tms_description:
            This test will verify that if a snapshot volume becomes invalid
            while the plan is running then the plan will fail.
        @tms_test_steps:
            @step: Create a snapshot of all nodes
            @result: snapshots are created
            @step: Backup lvconvert
            @result: lvconvert is backed up
            @step: Prepare dummy lvconvert file that exits with an error
            @result: Lvconvert is replaced
            @step: Run the litp restore_snapshot command
            @result: message log contains an error
            @result: The restore snapshot plan fails.
            @result: The snapshot timestamp is still there.
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Create a snapshot of all nodes')
        if not self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_createsnapshot(self.ms_node)

        for mn_node in self.mn_nodes:
            self.log('info', 'Backup/move lvconvert')
            self.backup_file(mn_node,
                             self.storage.lvconvert_path,
                             backup_mode_cp=False)

            self.log('info',
                'Prepare dummy lvconvert file that exits with an error')
            file_contents = ['#!/bin/sh',
                             'echo "lvconvert hit a problem 5" >&2',
                             "exit 5"]
            create_success = self.create_file_on_node(mn_node,
                self.storage.lvconvert_path, file_contents, su_root=True)
            self.assertTrue(create_success, "File could not be created")

        try:
            self.log('info', 'Run the litp restore_snapshot command')
            self.execute_cli_restoresnapshot_cmd(self.ms_node)

            self.log('info', 'Verify that the message log contains a '\
                    'message indicating why the snapshot failed to restore.')
            log_msg = "lvconvert hit a problem 5"
            self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))

            self.log('info', 'Verify that the restore snapshot plan fails.')
            self.assertTrue(self.wait_for_plan_state(
                self.ms_node,
                test_constants.PLAN_FAILED,
                self.timeout_mins
            ))

            self.log('info',
                'Verify that the snapshot timestamp is still there.')
            self.assertTrue(self._sshot_timestamp_exists())
        finally:
            # turn on everything that was disabled during the restore plan
            self._cleanup_after_failed_restore()

    @attr('all', 'revert', 'story2482', 'story2482_tc19')
    def test_19_n_restore_snapshot_fails_lvconvert_doesnt_return(self):
        """
        @tms_id: litpcds_2482_tc19
        @tms_requirements_id: LITPCDS-2482, LITPCDS-10877
        @tms_title: restore snapshot fails if lvconvert does not return
        @tms_description:
            This test will verify that the restore_snapshot plan fails and an
            error is generated if the lvconvert command doesn't return.
        @tms_test_steps:
            @step: Create a snapshot of all nodes
            @result: snapshots are created
            @step: Backup lvconvert
            @result: lvconvert is backed up
            @step: Prepare dummy lvconvert file that does not return within
                the task timeout
            @result: Lvconvert is replaced
            @step: Run the litp restore_snapshot command
            @result: message log contains an error
            @result: The restore snapshot plan fails.
            @result: The snapshot timestamp is empty
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Create a snapshot of all nodes')
        if not self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_createsnapshot(self.ms_node)

        try:
            self.log('info', 'Backup lvconvert')
            for node in self.mn_nodes:
                self.backup_file(node,
                             self.storage.lvconvert_path,
                             backup_mode_cp=False)

                self.log('info',
                    'Prepare dummy lvconvert file that does not return within'
                    ' the task timeout')
                file_contents = ["#!/bin/sh",
                                 "sleep 400"]
                create_success = self.create_file_on_node(
                    node,
                    self.storage.lvconvert_path,
                    file_contents,
                    su_root=True
                )

                self.assertTrue(create_success, "File could not be created")

            self.log('info', 'Run the litp restore_snapshot command')
            self.execute_cli_restoresnapshot_cmd(self.ms_node)
            log_msg = "execution expired"
            self.log('info', 'Verify that the message log contains a '\
                    'message indicating why the snapshot failed to restore.')

            self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))
            self.log('info', 'Verify that the restore snapshot plan fails.')
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                                    test_constants.PLAN_FAILED,
                                                    self.timeout_mins))

            self.log('info',
                'Verify that the snapshot timestamp is empty.')
            self.assertTrue(self._sshot_timestamp_exists())

        finally:
            self._cleanup_after_failed_restore()

    @attr('all', 'revert', 'story2482', 'story2482_tc26')
    def test_26_p_consecutive_restore_snapshots(self):
        """
        @tms_id: litpcds_2482_tc26
        @tms_requirements_id: LITPCDS-2482, LITPCDS-10877
        @tms_title: consecutive restore snapshot
        @tms_description:Verify that snapshots can be restored consecutively.
        @tms_test_steps:
            @step: Create a snapshot of all nodes
            @result: snapshots are created
            @step: Create a timestamped file on all snapshotted volumes
            @result: File is modified
            @step: Modify the grub backup files to contain a timestamp.
            @result: grup backup file has a timestamp
            @step: Run the litp restore_snapshot command
            @result: Restore_snapshot plan is created
            @step: Run stop_plan
            @result: Restore_snapshot plan cannot be stopped
            @result: LITP replies with InvalidRequestError
            @step: Wait for restore_snapshot plan to end
            @result: restore snapshot completes.
            @result: snapshots are restored
            @result: active grub file contains the timestamp
            @result: all snapshots are removed
            @step: Remove plan and snapshot (as per documented procedure)
            @result: Plan is removed
            @result: Remove snapshot plan succeeds
            @step: Create a snapshot of all nodes
            @result: snapshots are created
            @step: Create a timestamped file on all snapshotted volumes
            @result: File is modified
            @step: Modify the grub backup files to contain a timestamp.
            @result: grup backup file has a timestamp
            @step: Run the litp restore_snapshot command
            @result: Restore_snapshot plan is created
            @step: Run stop_plan
            @result: Restore_snapshot plan cannot be stopped
            @result: LITP replies with InvalidRequestError
            @step: Wait for restore_snapshot plan to end
            @result: restore snapshot completes.
            @result: snapshots are restored
            @result: active grub file contains the timestamp
            @result: all snapshots are removed

        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        self._restore_snapshot()
        self.log('info',
        'Remove plan and snapshot (as per documented procedure)')
        self.execute_cli_removeplan_cmd(self.ms_node)

        self.log('info', 'Create a snapshot of all nodes.')
        self.execute_and_wait_createsnapshot(self.ms_node)

        self._restore_snapshot()

    @attr('all', 'revert', 'story2482', 'story2482_tc24')
    def test_24_n_corrupt_snapshot_returns_error(self):
        """
        @tms_id: litpcds_2482_tc24
        @tms_requirements_id: LITPCDS-2482, LITPCDS-10877
        @tms_title: restore snapshot fails on corrupt snapshot
        @tms_description:
            This test will verify that if a snapshot volume becomes corrupt
            then the restore_snapshot command will fail.
        @tms_test_steps:
            @step: Set the snap_size of the root FS on a managed node to be 1%.
            @result: Snap_size is set to 1%
            @step: Create a new snapshot of all nodes
            @result: Snapshots are created
            @step: Fill the filesystem with snapsize=1
            @result: Filesystem becomes invalid
            @step: Run the litp restore_snapshot command
            @result: Restore_snapshot plan fails
            @result: Task to check for snapshot validity fails
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # VALIDATION ADDED TO LITPCDS-8716 PREVENTS SNAP_SIZE CHANGES WHILE
        # A SNAPSHOT EXISTS IN THE MODEL, THUS WE MUST REMOVE THE SNAP BEFORE
        # PROCEEDING WITH THE TEST.
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        # Save the value of the root volumes snap_size value
        mfs = self.get_all_volumes(self.ms_node, vol_driver='lvm')
        fsystem = None
        for fsystem in mfs:
            if fsystem["volume_name"] == 'root':
                fs_found = True
                break
        self.assertTrue(fs_found)

        self.log('info',
                'Set the snap_size of root fs on a managed node to be 1.')
        self.backup_path_props(self.ms_node, fsystem['path'])
        self.execute_cli_update_cmd(self.ms_node,
                                    fsystem['path'],
                                    "snap_size=1")

        self.log('info', 'Create a new snapshot of all nodes')
        self.execute_and_wait_createsnapshot(self.ms_node)

        try:
            self.log('info',
            'Fill the filesystem with snapsize = 1'
            'so that it becomes invalid.')
            meg_size = 12 * self.sto.convert_size_to_megabytes(
                    "{0}".format(fsystem['size']))
            cmd = (
                "/bin/dd if=/dev/urandom of=/tmp/2482_file bs=1024 count={0}"
                .format(meg_size))
            _, _, rc = self.run_command(fsystem['node_name'], cmd)
            self.assertEqual(0, rc)

            # Wait for LVM to notice fs change
            count = 60
            out = list()
            while (count > 0 and
            not self.is_text_in_list("Input/output error", out)):
                out, _, _ = self.run_command(fsystem['node_name'],
                        "/sbin/vgscan", default_asserts=True, su_root=True)
                count -= 1
                time.sleep(1)

            self.log('info', 'Run the litp restore_snapshot command')
            self.execute_cli_restoresnapshot_cmd(self.ms_node)
            self.wait_for_plan_state(self.ms_node,
                                     test_constants.PLAN_FAILED)

            # 6. Verify that VALIDITY CHECK FAILED
            lvm_snapshot_task_state = \
                self.get_task_state(self.ms_node,
                'Check LVM snapshots on node(s) "node2 and node1"')
            self.assertEqual(test_constants.PLAN_TASKS_FAILED,
                lvm_snapshot_task_state)

        finally:
            # Remove the dummy file.
            cmd = "/bin/rm /tmp/2482_file"
            self.run_command(fsystem['node_name'], cmd, default_asserts=True)
            self._cleanup_after_failed_restore()
