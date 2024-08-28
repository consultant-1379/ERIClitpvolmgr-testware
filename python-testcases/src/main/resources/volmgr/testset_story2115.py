"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     March 2014
@author:    Padraic Doyle
@summary:   Integration test for story 2115 As a LITP admin I want to
            snapshot LVM volumes present in my deployment when I am doing
            maintenance operations so that I can revert to them later on.
            Agile: STORY-2115
"""
from litp_generic_test import GenericTest, attr
from storage_utils import StorageUtils
import test_constants
import time
import math
import re


class Story2115(GenericTest):
    """
    As a LITP admin I want to snapshot LVM volumes present in my
    deployment when I am doing maintenance operations so that I can revert
    to them later on.

    Update for story LITPCDS-12294:
    The currrent test_01_p_user_can_create_snapshots_for_all_logical_volumes
    maps to test_02_p_list_logical_volumes_ms in LITPCDS-12294.
    """

    def setUp(self):
        """Setup variables for every test"""
        super(Story2115, self).setUp()
        self.ms_nodes = self.get_management_node_filenames()
        self.ms_node = self.ms_nodes[0]
        self.mn_nodes = self.get_managed_node_filenames()
        self.all_nodes = self.ms_nodes + self.mn_nodes
        self.timeout_mins = 10
        self.storage = StorageUtils()

    def tearDown(self):
        """Runs for every test"""
        super(Story2115, self).tearDown()

    def _verify_snapshots(self):
        """
        Description:
            Verify logical volume snapshots are created
        Actions:
            For all nodes
                For all logical volumes
                    1. Verify that a snapshot is created.
        Results:
            stdmsg, stderr
        """
        # Added local variables for LITPCDS-12294
        ms_lv_to_check = ['lv_root', 'lv_home', 'lv_software', 'lv_swap',
         'lv_var', 'lv_var_lib_puppetdb', 'lv_var_log', 'lv_var_www',
                          'lv_var_opt_rh', 'lv_var_tmp']
        ms_snap_to_check = ['lv_var_www', 'lv_var', 'lv_root', 'lv_home',
                        'lv_var_lib_puppetdb', 'lv_var_tmp', 'lv_var_opt_rh']
        # Is used to compare the expected lv in the ms against what we have in
        # the the ms. The end of the test in AssertEqual statement
        ms_lv_snap_found = []
        ms_lv_found = []

        lvs_cmd = self.storage.get_lvs_cmd(grep_args=r"-v 'swi\|LSize'")
        # Get the logical volumes
        lvols = []
        for node in self.all_nodes:
            # Get the logical volumes
            lvs_out, err, ret_code = self.run_command(node, lvs_cmd,
                                                      su_root=True)
            self.assertEqual([], err)
            self.assertTrue(ret_code < 2)
            if lvs_out:
                log_vols = self.storage.parse_lvs_stdout(lvs_out)
                for log_vol in log_vols:
                    self.log("info", "lv_name is {0}".format(log_vol['LV']))
                    lvols.append([node, log_vol['LV']])
                    if node == self.ms_node:
                        ms_lv_found.append(log_vol['LV'])

        # Verify that each logial volume has a snapshot, except for 'swap'
        # and 'software'. var/log is snapshot only if it name snapshot
        # 'create snapshot -n'
        for lvol in lvols:
            expect_snapshot = True
            no_snap_list = ['swap', 'var_log', 'software']
            for lvname in no_snap_list:
                if lvname in lvol[1]:
                    # no snapshots for swap , var_log and software
                    expect_snapshot = False

            # Verify that there is a corresponding snapshot for all LVs
            cmd_args = "swi | grep {0}".format(lvol[1])
            cmd = self.storage.get_lvs_cmd(grep_args=cmd_args)
            lvs_out, err, ret_code = self.run_command(lvol[0], cmd,
                                                      su_root=True)
            self.assertEqual([], err)
            self.assertEqual(expect_snapshot,
                    self.is_text_in_list(lvol[1], lvs_out))
            self.assertNotEqual(expect_snapshot, ret_code)

            if expect_snapshot and lvol[0] == self.ms_node:
                ms_lv_snap_found.append(lvol[1])

        # Verify the lv on the ms is according to LITPCDS-12294
        self.assertEqual(set(ms_lv_to_check), set(ms_lv_found))
        self.assertEqual(set(ms_snap_to_check), set(ms_lv_snap_found))

    def _verify_grub(self):
        """
        Description:
            Verify grub backup
        Actions:
            For all nodes
                1. Verify that a grub.conf is backed_up.
        Results:
            stdmsg, stderr
        """
        cmp_cmd = "cmp {0} {0}.backup".format(test_constants.GRUB_CONFIG_FILE)
        for node in self.all_nodes:
            out, err, ret_code = self.run_command(node, cmp_cmd, su_root=True)
            self.assertEqual([], err)
            self.assertTrue(ret_code < 2)
            self.assertEqual([], out)

    def _get_model_file_systems(self):
        """
        Description:
            Return a list of lists representing the file systems in the model
        Actions:
            1. Get software items collection path
            2. Create test package
        Results:
        """
        fsystems = []
        nodes_urls = self.find(self.ms_node, "/deployments", "node")

        for node_path in nodes_urls:
            self.log('info',
                'Get the hostname of the node so we can match its attributes')
            node_props = self.get_props_from_url(self.ms_node, node_path)
            self.assertFalse(node_props is None)
            node_hostname = node_props["hostname"]

            # GET NODE STORAGE PROFILE NAME
            storage_path = node_path + "/storage_profile"
            storage_props = self.get_props_from_url(self.ms_node,
                                                    storage_path)
            self.assertFalse(storage_props is None)

            # Get volume groups
            vgs = self.find(self.ms_node, storage_path, "volume-group")
            for vg_path in vgs:
                lv_prefix = vg_path.split('/')[-1]
                vg_props = self.get_props_from_url(self.ms_node,
                                                   vg_path)
                self.assertFalse(vg_props is None)
                fss = self.find(self.ms_node, vg_path, "file-system")

                for fs_path in fss:
                    fsystem = [node_hostname]
                    # Get file system name from url
                    fs_name = fs_path.split("/")[-1]
                    fsystem.append(fs_name)
                    fs_props = self.get_props_from_url(self.ms_node,
                                                       fs_path)
                    self.assertFalse(fs_props is None)
                    fsystem.append(fs_props["type"])
                    fsystem.append(
                        self.storage.convert_size_to_megabytes(
                            fs_props["size"]))
                    fsystem.append(fs_props["snap_size"])
                    fsystem.append("L_{0}_{1}_".format(lv_prefix,
                                                      fs_name))
                    fsystems.append(fsystem)

        return fsystems

    def _get_nodes_file_systems(self):
        """
        Description:
            Return a list of lists representing the file systems.
        Actions:
            1. Get software items collection path
            2. Create test package
        Results:
            [file_systems_dict1, file_systems_dict2, ...)
            'node': node_hostname,
            'fs_name': file_system_name,
            'location': location_in_model N/A,
            'snap_name': snapshot_volume_name,
            'snap_size': snapshot_percentage_of_origin,
            'type': 'swap',
            'size': '2G'
        """
        fsystems = []

        for node in self.mn_nodes:
            hostname = self.get_node_att(node, "hostname")

            cmd = self.storage.get_lvdisplay_cmd("-c")
            out, err, ret_code = self.run_command(node, cmd,
                                                  su_root=True)
            self.assertEqual([], err)
            self.assertEqual(0, ret_code)
            for line in out:
                fsystem = [hostname]
                f_vol_output = line.strip().split(':')
                lv_path = f_vol_output[0]

                # Get the LV Name
                cmd = self.storage.get_lvdisplay_cmd(lvdisplay_args=lv_path,
                                                       grep_args="\"LV Name\"")
                out, err, ret_code = self.run_command(node, cmd,
                                                      su_root=True)
                self.assertEqual([], err)
                self.assertEqual(0, ret_code)
                fsystem.append(out[0].split()[2])

                # Get the FS type from the block ID command
                cmd = '/sbin/blkid -s TYPE -o value {0}'.format(lv_path)
                out, err, ret_code = self.run_command(node, cmd,
                                                      su_root=True)
                self.assertEqual([], err)
                self.assertEqual(0, ret_code)
                fsystem.append(out[0])

                # Get the LV Size
                fs_name = lv_path.split("/")[-1]
                lvs_cmd = r"/sbin/lvs --units g | grep {0}".format(fs_name)
                out, err, ret_code = self.run_command(node, lvs_cmd,
                                                      su_root=True)
                self.assertEqual([], err)
                self.assertTrue(ret_code < 2)
                if out:
                    lv_size = float(out[0].split()[3][:-1]) * 1024
                    self.log("info", "{0} size = {1}".format(fs_name, lv_size))
                    fsystem.append(lv_size)

                fsystems.append(fsystem)

        return fsystems

    @staticmethod
    def _approx_equal(var_x, var_y, tolerance=0.01):
        """ Approximate comparison. """
        return abs(var_x - var_y) <= 0.5 * tolerance * (var_x + var_y)

    @attr('all', 'revert', 'story2115', 'story2115_tc01')
    def test_01_p_user_can_create_snapshots_for_all_logical_volumes(self):
        """
        @tms_id: litpcds_2115_tc01
        @tms_requirements_id: LITPCDS-2115
        @tms_title: litp create_snaphot LVM
        @tms_description:
            This test will verify that when a user runs create_snapshot, tasks
            for all the LVM volumes in the model are created and executed
            successfully.
            Update TC for story LITPCDS-12294:
                Given a LITP deployment
                WHEN I run create_snapshot
                THEN /var/log and /software are not snapshot in MS
            This TC maps into test_02_p_list_logical_volumes_ms in
            LITPCDS-12294.
        @tms_test_steps:
            @step: Create a snasphot
            @result: Snapshot plan succeeds
            @result: Snapshots are created on each node
            @result: Grub is backed up
            @result: Timestamp is the current time
            @result: Snapshots have the correct name and size
            @step: Attempt to create another snapshot
            @result: the user is given information on existing snapshot.
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Create a snapshot')
        self.execute_and_wait_createsnapshot(self.ms_node)

        self.log('info',
            'Verify that the snapshot volumes are created on each node.')

        self._verify_snapshots()

        self.log('info', 'Verify that grub is backed up')
        self._verify_grub()

        self.log('info',
            'Verify that the snapshot item is created at the top level.')
        sshot = self.find(self.ms_node, "/snapshots",
                          "snapshot-base", assert_not_empty=False)

        self.assertEqual("/snapshots/snapshot", sshot[0])

        self.log('info', 'Verify that the time-stamp is the current time.')
        props_dict = self.get_props_from_url(self.ms_node, sshot[0])

        ss_time = float(props_dict['timestamp'])
        curr_time = time.time()
        self.log("info", "Snapshot time is: {0}".format(ss_time))
        self.log("info", "Current time is : {0}".format(curr_time))
        self.assertTrue((curr_time - 180) < ss_time < (curr_time + 180),
                        "Timestamp is not within 3 minutes of current time")

        # Get the disk volumes from the model as a list of attribute lists
        #    eg.[['mn1', 'root', 'xfs', '16G', '50', 'L_root_'], [
        model_disks = self._get_model_file_systems()

        # Get the disk volumes from the nodes as a list of attribute lists
        #    e.g. [['mn1', 'root', 'xfs', '8.00'],[...]]
        actual_disks = self._get_nodes_file_systems()

        self.log('info',
            'Verify that the snapshot was created with correct name and size.')
        for model_disk in model_disks:
            # If it's the correct type
            if (model_disk[2] == 'xfs') and (model_disk[4] != '0'):
                snapshot_present = False
                # Verify that there is a snapshot of the correct size.
                for actual_disk in actual_disks:
                    if ((model_disk[5] == actual_disk[1]) and
                        (model_disk[0] == actual_disk[0])):
                        self.log("info", "Vol {0} on {1} has snapshot {2}"
                              .format(model_disk[1],
                                      model_disk[0],
                                      actual_disk[1]))
                        snapshot_present = True
                        # The expected size of the snapshot disk is the
                        #  original disk size times the percentage snap_size.
                        expect_dsk_sz = (float(model_disk[3]) *
                                              (float(model_disk[4]) / 100.0))
                        self.log("info", "Expect {0} to be {1}".
                                 format(model_disk[0], expect_dsk_sz))

                        if self._approx_equal(expect_dsk_sz, actual_disk[3]):
                            self.log("info", "Size for {0}, {1} == {2}"
                                .format(model_disk[1], expect_dsk_sz,
                                float(actual_disk[3])))
                        else:
                            self.log("info", "Size for {0}, {1} != {2}"
                                     .format(model_disk[1],
                                             expect_dsk_sz,
                                             actual_disk[3]))
                        self.assertTrue(self._approx_equal(expect_dsk_sz,
                                                           actual_disk[3]))
                if not snapshot_present:
                    self.log("info", "No snapshot for {0}".format(model_disk))
                self.assertTrue(snapshot_present)

        date_cmd = "/bin/date -d@{0} +'%a %b %_d %T %Y'".format(ss_time)
        out, _, _ = self.run_command(self.ms_node, date_cmd,
                default_asserts=True)

        self.log('info', 'Attempt to create another snapshot.')
        _, err, _ = self.execute_cli_createsnapshot_cmd(self.ms_node,
                                                        expect_positive=False)

        self.log('info',
            'Verify that the user is given information on existing snapshot.')
        expected_err = ("DoNothingPlanError    no tasks were generated. "
                       "No snapshot tasks added because Deployment Snapshot " +
                       "with timestamp {0} ".format(out[0]))

        self.assertTrue(self.is_text_in_list(expected_err, err), "{0} != {1}"
                        .format(expected_err, err))

    @attr('all', 'revert', 'story2115', 'story2115_tc04')
    def test_04_n_error_reported_if_snapshot_exists(self):
        """
        @tms_id: litpcds_2115_tc04
        @tms_requirements_id: LITPCDS-2115
        @tms_title: error if snapshot already existing
        @tms_description:
            This test will verify that an error is reported if a snapshot of a
            volume already exists on a node.
        @tms_test_steps:
            @step: Manually create a lvm snapshot with the snapshot target
                   name on a node.
            @result: Manual snapshot is created
            @step: Run the create_snapshot command
            @result: Snapshot plan fails
            @result: There is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        lvm_fs = self.get_all_volumes(self.ms_node, vol_driver="lvm")
        node = self.mn_nodes[0]
        vg_id = (x['vg_item_id'] for x in lvm_fs if x['node_name'] ==
                node).next()

        # Delete the existing snapshot
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        self.log('info',
            'Manually create a snapshot with the target name on a node.')
        ss_name = "L_{0}_root_ ".format(vg_id)
        args = ("-L 100M -s -n {0} ".format(ss_name) +
                test_constants.LITP_SNAPSHOT_PATH + "{0}_root".format(vg_id))
        cmd = self.storage.get_lvcreate_cmd(args)

        out, _, _ = self.run_command(node, cmd, su_root=True,
                default_asserts=True)
        self.assertTrue(self.is_text_in_list('created', out))

        self.log('info', 'Run the create_snapshot command')
        self.execute_cli_createsnapshot_cmd(self.ms_node)

        self.log('info',
            'Verify that the message log contains a message indicating why '
            'the snapshot failed to create.')

        log_msg = "already exists in volume group"
        self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))

        self.log('info', 'Verify that the plan fails.')
        plan_failed = self.wait_for_plan_state(
            self.ms_node,
            test_constants.PLAN_FAILED,
            self.timeout_mins
        )
        self.assertTrue(plan_failed, "Plan was successful")

    @attr('all', 'revert', 'story2115', 'story2115_tc05')
    def test_05_n_error_reported_if_not_enough_space_for_a_snapshot(self):
        """
        @tms_id: litpcds_2115_tc05
        @tms_requirements_id: LITPCDS-2115
        @tms_title: error if not enough space for snapshot
        @tms_description:
            This test will verify that the task for snapshotting a volume fails
            if there is not enough space available for the snapshot.
        @tms_test_steps:
            @step:Create a 'filler' volume
            @result: Filler volume is created
            @step: Run the create_snapshot command
            @result: Snapshot plan fails
            @result: There is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # Delete the existing snapshot
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        fss = self.get_all_volumes(self.ms_node, vol_driver="lvm")
        node = self.mn_nodes[0]
        fsystem = (x for x in fss if
                x['node_name'] == node
                and
                x['volume_name'] == 'root').next()
        try:
            self.log('info', 'Create a filler volume')
            space = re.sub('[<]', '', (self.get_vg_info_on_node(
                node)[fsystem['volume_group_name']]['VG_SIZE_FREE_GB']))
            space = self.storage.convert_size_to_megabytes(
                    "{0} G".format(math.ceil(float(space))))

            self.log('info', 'Get the size of root volume')
            lv_size = self.storage.convert_size_to_megabytes(fsystem['size'])
            self.log("info", "lv_size is {0}".format(lv_size))

            self.log('info', 'Get the value of snap_size for root volume')
            snap_size_percent = fsystem['snap_size']

            snap_size = float(snap_size_percent) / 100

            # Calculate filler size so that there is only 95% of space
            # required for a snapshot.
            # filler = unused_space - (0.95 * ( root_vol_size * snap_size))
            filler_size = float(space) - (0.95 *
                    (float(lv_size) * (snap_size)))

            self.log("info", "Filler_size is {0}".format(filler_size))

            args = "-L {0}MiB -n filler {1} -y".format(filler_size,
                fsystem['volume_group_name'])
            cmd = self.storage.get_lvcreate_cmd(args)

            out, _, _ = self.run_command(node, cmd, su_root=True,
                    default_asserts=True)
            self.assertTrue(self.is_text_in_list('created', out))

            self.log('info', 'Run create a snapshot.')
            self.execute_cli_createsnapshot_cmd(self.ms_node)

            # Expected message in message log
            log_msg = ('has insufficient free space')
            self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))

            self.log('info',
                'Verify that the plan fails for the node with the filler.')
            plan_failed = self.wait_for_plan_state(
                self.ms_node,
                test_constants.PLAN_FAILED,
                self.timeout_mins
            )
            self.assertTrue(plan_failed, "Plan was successful")

        finally:
            self.log('info', 'Remove filler volume')
            filler_vol = "/dev/{0}/filler".format(fsystem['volume_group_name'])
            cmd = self.storage.get_lvremove_cmd(filler_vol, "-f")
            out, _, _ = self.run_command(node, cmd,
                                                su_root=True,
                                                default_asserts=True)
            self.assertTrue(self.is_text_in_list("successfully removed",
                                                out))

    @attr('all', 'revert', 'story2115', 'story2115_tc14')
    def test_14_n_error_returned_lvcreate_command_complete(self):
        """
        @tms_id: litpcds_2115_tc14
        @tms_requirements_id: LITPCDS-2115
        @tms_title: error if lvcreate does not return
        @tms_description:
            This test will verify that an error is returned by litp if the
            lvcreate command doesn't return.
        @tms_test_steps:
            @step: Replace lvcreate with one that does not return within the
                   task timeout
            @result: lvcreate is replaced
            @step: Run the create_snapshot command
            @result: Snapshot plan fails
            @result: There is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        node = self.mn_nodes[0]

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        self.log('info', 'Replace lvcreate with one that does not return '
                'within the task timeout')
        lvcreate_path = self.storage.lvcreate_path
        self.backup_file(node, lvcreate_path, backup_mode_cp=False)

        file_contents = ["#!/bin/bash", "sleep 400"]
        create_success = self.create_file_on_node(node, lvcreate_path,
                                                  file_contents,
                                                  su_root=True)
        self.assertTrue(create_success, "File could not be created")

        self.log('info', 'Run the create_snapshot')
        self.execute_cli_createsnapshot_cmd(self.ms_node)

        self.log('info',
            'Verify that there is an error message in the message log.')

        log_msg = ('execution expired')
        self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))

        plan_failed = self.wait_for_plan_state(
            self.ms_node,
            test_constants.PLAN_FAILED,
            self.timeout_mins)

        self.log('info',
            'Verify the plan fails for the node with the dummy lvcreate')
        self.assertTrue(plan_failed, "Plan was not successful")
