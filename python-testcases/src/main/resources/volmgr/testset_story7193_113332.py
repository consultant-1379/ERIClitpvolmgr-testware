'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     January  2015
@author:    Artur, Adrian
@summary:   Integration
            Agile: LITPCDS-7193, TORF-113332
'''

from litp_generic_test import GenericTest, attr
from litp_cli_utils import CLIUtils
from storage_utils import StorageUtils
import test_constants
import time
import os


class Story7193(GenericTest):
    """
    LITPCDS-7193
    As a LITP User
    I want ability to specify a name tag to the remove_snapshot
    command so that I can easily delete named snapshots

    TORF-113332
    As a LITP user, I want to be able to specify a snap_size for my
    backup snapshots independent of my snap_size for my deployment
    snapshots so that I can take snapshots of varying size
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story7193, self).setUp()
        # 2. Set up variables used in the test
        self.ms_nodes = self.get_management_node_filenames()
        self.ms_node = self.ms_nodes[0]
        self.mn_nodes = self.get_managed_node_filenames()
        self.all_nodes = self.ms_nodes + self.mn_nodes
        self.timeout_mins = 10
        self.cli = CLIUtils()
        self.storage = StorageUtils()

    def tearDown(self):
        """Runs for every test"""
        super(Story7193, self).tearDown()

    def _get_vxvm_snapshots(self, nodes, full_path=False):
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
        dg_names = [vg["volume_group_name"] for vg in
                self.get_all_volumes(self.ms_node)]
        for dg_name in dg_names:
            cmd = self.storage.get_vxsnap_cmd(dg_name, grep_args="L_")
            for node in nodes:
                out, err, ret_code = self.run_command(node, cmd, su_root=True)
                self.assertEqual([], err)
                self.assertTrue(ret_code < 2)
                if out:
                    for line in out:
                        if 'L_' in line:
                            if not full_path:
                                sshots.append(line.split(" ")[0])
                            else:
                                sshots.append([line.split(" ")[0],
                                    dg_name, node])
        return sshots

    def _create_named_snapshot(self, ss_name):
        """ create_named_snapshot """

        # 1. Execute create_snapshot command
        if self.is_snapshot_item_present(self.ms_node, ss_name):
            self.execute_and_wait_removesnapshot(self.ms_node,
                "-n {0}".format(ss_name))

        args = "--name {0}".format(ss_name)
        self.execute_cli_createsnapshot_cmd(self.ms_node, args)

        # 2 Verify that the create snapshot plan succeeds
        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                test_constants.PLAN_COMPLETE, self.timeout_mins))

    def _verify_grub_dir(self, dir_contents):
        """ Verify grub_dir """
        out = self._get_grub_dir_contents()
        self.assertEqual(dir_contents, out)

    def _reduce_snap_sizes(self, fss):
        """ reduce snap sizes"""
        for fsys in fss:
            if fsys["type"] == "xfs":
                self.backup_path_props(self.ms_node, fsys["path"])
                cmd = self.cli.get_update_cmd(fsys["path"],
                                              "snap_size=1")
                self.run_command(self.ms_node, cmd, default_asserts=True)

    def _get_grub_dir_contents(self):
        """ Get the Grub directory contents """
        grub_dir, _ = os.path.split(test_constants.GRUB_CONFIG_FILE)
        cmd = "/bin/ls -l {0}".format(grub_dir)
        out, _, _ = self.run_command(self.ms_node, cmd, su_root=True,
                default_asserts=True)
        return out

    def _verify_snap_sizes(self, fss):
        """
        Verifies the size of modelled lvm file systems
        """

        for node in self.mn_nodes:
            fsystem = self.get_lv_info_on_node(node)
            snapshots = dict([(key, details) for key, details in
                fsystem.iteritems() if details.get('LV_SNAP_STATUS') and
                'destination' in details.get('LV_SNAP_STATUS')])
            for _, snap_details in snapshots.iteritems():
                modelled_fs = (x for x in fss if x['node_name'] == node and
                    x['volume_group_name'] == snap_details['VG_NAME']).next()

                actual_size = float(snap_details['COW_TABLE_SIZE_MB'])

                if modelled_fs.get('backup_snap_size'):
                    expected_size = float(modelled_fs['backup_snap_size']) *\
                            0.01 *\
                    self.sto.convert_size_to_megabytes(modelled_fs['size'])
                else:
                    expected_size = float(modelled_fs['snap_size']) *\
                        0.01 * self.sto.convert_size_to_megabytes(
                                modelled_fs['size'])

                error = abs((actual_size - expected_size) /\
                    float(max(actual_size, expected_size)))
                self.assertTrue(error <= 0.05)

    @attr('all', 'revert', 'story7193', 'story113332', 'story7193_tc01',
            'story113332_tc01')
    def test_01_p_all_snapshots_removed(self):
        """
        @tms_id: litpcds_7193_tc01
        @tms_requirements_id: LITPCDS-7193
        @tms_title: create/remove named snapshots
        @tms_description:
            This tests verifies creation/removal of named snapshots
            This covers TORF-113332
        @tms_test_steps:
            @step: Change backup_snap_size of file system to a random positive
                value
            @result: backup_snap_size is updated
            @step: Create a named snapshot
            @result: A named snapshot item is created in the model
            @result: The snapshot item is in state Applied
            @result: The snapshot item time stamp has the current time
            @result: There is no grub backup
            @step: Do an xml export of the model
            @result: The snapshot item is not exported
            @result: Actual snap sizes are according to the modelled values
            @step: Remove the named snapshot
            @result: All snapshot volumes have been removed
            @result: The snapshot item has been removed
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        fss = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        self.log('info',
        'Change backup_snap_size of file system to a random positive value')

        dont_add_backup_snap_size = True
        snap_size = 55
        for fsystem in fss:
            if fsystem['type'] != 'xfs':
                continue

            self.backup_path_props(self.ms_node, fsystem['path'])
            # only update some file systems
            if dont_add_backup_snap_size:
                # leave one fs without backup_snap_size
                dont_add_backup_snap_size = False
                self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                    props='snap_size={0}'.format(snap_size))
            else:
                self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                    props='backup_snap_size={0}'.format(snap_size))
        snap_name = "test_01_01"
        snap_path = "/snapshots/" + snap_name

        #update the structure with the updated values
        fss = self.get_all_volumes(self.ms_node, vol_driver='lvm')
        if self.is_snapshot_item_present(self.ms_node):
            self.remove_all_snapshots(self.ms_node)

        boot_dir_contents = self._get_grub_dir_contents()

        self.log('info', 'Create a named snapshot')
        self._create_named_snapshot(snap_name)

        # Verify that there is a new 'named' snapshot item.
        self.assertTrue(self.is_snapshot_item_present(self.ms_node, snap_name))

        # Verify that the snapshot item is Applied.
        self.assertEqual("Applied", self.get_item_state(self.ms_node,
            snap_path))

        # Verify that the time-stamp is the current time.
        props_dict = self.get_props_from_url(self.ms_node, snap_path)
        ss_time = float(props_dict['timestamp'])
        curr_time = time.time()
        self.log("info", "Snapshot time is: {0}".format(ss_time))
        self.log("info", "Current time is : {0}".format(curr_time))
        self.assertTrue((curr_time - 180) < ss_time < (curr_time + 180),
                        "Timestamp isn't within 3 minutes of current time")

        # Verify that there is no new grub backup.
        self._verify_grub_dir(boot_dir_contents)

        self.log('info', 'Do an xml export of the model')
        stdout, _, _ = self.execute_cli_export_cmd(self.ms_node, "/")

        # Verify that the snapshot item is not being exported
        self.assertFalse(self.is_text_in_list(snap_name, stdout))

        # Verify snap sizes
        self._verify_snap_sizes(fss)

        self.log('info', 'Remove the named snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node,
            args='--name {0}'.format(snap_name))

        stdout, _, _ = self.execute_cli_showplan_cmd(self.ms_node)

        # Check that the snapshots have been removed
        fss = self.get_all_volumes(self.ms_node)

        for filesys in fss:
            vx_disk_node = self.get_vx_disk_node(self.ms_node,
                    disk_group=filesys["volume_group_name"])

            if filesys["snap_size"] != '0':
                cmd = self.storage.get_vxsnap_cmd(filesys['volume_group_name'],
                                                  grep_args="L_")
                out, err, _ = self.run_command(vx_disk_node,
                                               cmd,
                                               su_root=True)
                expect_ss_vol = "L_" + filesys["volume_name"] + \
                             "_" + snap_name
                self.log("info", "Expect snapshot '{0}'".format(expect_ss_vol))
                self.assertFalse(self.is_text_in_list(expect_ss_vol, out))

                self.assertFalse(self.is_text_in_list("ERROR", err))
        sshot_list = self.get_snapshots(self.all_nodes)
        self.assertEqual([], sshot_list)

        # Verify that the named snapshot item is removed from /snapshots
        self.assertFalse(self.is_snapshot_item_present(
            self.ms_node, snap_name))

    @attr('all', 'revert', 'story7193', 'story7193_tc02', 'kgb-physical')
    def test_02_p_remove_lvm_snapshots(self):
        """
        @tms_id: litpcds_7193_tc02
        @tms_requirements_id: LITPCDS-7193
        @tms_title: create/remove two named snapshots
        @tms_description:
            This test verifies creation/removal of two named snapshots
        @tms_test_steps:
            @step: Create two named snapshot
            @result: Snapshots have been created
            @step: Remove the first created named snapshot
            @result: The snapshot volumes of the first named snapshot are
                removed
            @result: The snapshot volumes of the second volume are still
                present
            @result: The snapshot item of the first snapshot is deleted
            @step: Remove the second named snapshot
            @result: Snapshot plan finishes successful
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        snap_names = ["test_02_01", "test_02_02"]

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)
        fss = self.get_all_volumes(self.ms_node, vol_driver='lvm')
        self._reduce_snap_sizes(fss)
        self.log('info', 'Create two named snapshots')
        self._create_named_snapshot(snap_names[0])
        self._create_named_snapshot(snap_names[1])

        self.log('info', 'Remove the first created named snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node,
                "-n {0}".format(snap_names[0]))

        sshot_list = self.get_snapshots(self.all_nodes)
        for sshot in sshot_list:
            self.assertTrue(snap_names[1] in sshot[1])

        # Verify that the named snapshot item is removed from /snapshots
        self.assertFalse(self.is_snapshot_item_present(
            self.ms_node, snap_names[0]))

        self.log('info', 'Remove the second named snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node,
                "-n {0}".format(snap_names[1]))

    @attr('all', 'revert', 'story7193', 'story7193_tc03', 'kgb-physical')
    def test_03_p_remove_snapshot_no_snapshot(self):
        """
        @tms_id: litpcds_7193_tc03
        @tms_requirements_id: LITPCDS-7193
        @tms_title: force remove snapshot
        @tms_description:
            This tests verifies remove snapshot -f succeeds if there are
            snapshots missing
            NOTE: Also verifies LITPCDS-12590
        @tms_test_steps:
            @step: Create a named snapshot
            @result: Snapshot has been created
            @step: Manually delete snapshots on one node
            @result: Snapshot has been deleted
            @step: Remove the named snapshot
            @result: Snapshot plan succeeds
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        snap_name = "test03"

        self.log('info', 'Create a named snapshot')
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)
        self._create_named_snapshot(snap_name)

        node = self.mn_nodes[0]
        sshots = self.get_snapshots([node])
        self.assertTrue(len(sshots) > 0)

        self.log('info', 'Manually delete snapshots on one node')
        for sshot in sshots:
            cmd = self.storage.get_lvremove_cmd(sshot[1], "-f")
            out, _, _ = self.run_command(node, cmd,
                                                  su_root=True,
                                                  default_asserts=True)
            self.assertTrue(self.is_text_in_list("successfully removed",
                                                 out))

        vxvm_snaps = self._get_vxvm_snapshots(self.mn_nodes, True)
        for snap in vxvm_snaps:
            args = "dis {0} ".format(snap[0])
            cmd = self.storage.get_vxsnap_cmd(snap[1], args)
            self.run_command(snap[2], cmd, su_root=True,
                    default_asserts=True)

            args = " -rf rm {0}".format(snap[0])
            cmd = self.storage.get_vxedit_cmd(snap[1], args)
            self.run_command(snap[2], cmd, su_root=True,
                    default_asserts=True)
        self.log('info', 'Remove the named snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node,
            "--name {0}".format(snap_name))
        # LITPCDS-12590: check that there is no snapshot leftovers
        cmd = "/sbin/vxprint | /bin/grep {0}".format(snap_name)
        for node in self.mn_nodes:
            out, _, _ = self.run_command(node, cmd)
            self.assertTrue(out == [], "snapshot cache was not removed")

    @attr('all', 'revert', 'story7193', 'story7193_tc16')
    def test_16_n_remove_failed_snapshot(self):
        """
        @tms_id: litpcds_7193_tc16
        @tms_requirements_id: LITPCDS-7193
        @tms_title: run remove_snapshot after failed remove_snapshot
        @tms_description:
            Test that even if a previous attempt to remove a snapshot
            has failed the snapshot can still be removed afterwards.
        @tms_test_steps:
            @step: Create named snapshot
            @result: Named snapshot has been created
            @step: Backup lvremove binary
            @result: lvremove binary has been backed up
            @step: Create a dummy executable for lvremove, that returns an
            error
            @result: lvremove is replaced
            @step: Remove the named snapshot
            @result: Remove snapshot plan fails
            @step: Restore the lvremove binary
            @result: lvremove binary has been restored
            @step: Remove the named snapshot
            @result: Remove snapshot plan succeeds
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        snap_name = "test_16_01"

        mn_node = self.mn_nodes[0]
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)
        self.log('info', "Create named snapshot")
        self._create_named_snapshot(snap_name)

        self.log('info', 'Backup lvremove binary')
        lvremove_path = self.storage.lvremove_path
        self.backup_file(mn_node, lvremove_path, backup_mode_cp=False)

        self.log('info',
            'Create a dummy executable for lvremove, that returns an error')

        file_contents = ['#!/bin/sh',
                         'echo "lvremove hit a problem 7193_4" >&2',
                         "exit 4"]
        self.assertTrue(self.create_file_on_node(mn_node,
                                                  self.storage.lvremove_path,
                                                  file_contents,
                                                  su_root=True,
                                                  add_to_cleanup=False),
                        "File could not be created")

        try:
            self.log('info', 'Remove the named snapshot')
            self.execute_cli_removesnapshot_cmd(self.ms_node,
                "-n {0}".format(snap_name))
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                test_constants.PLAN_FAILED, self.timeout_mins))

        finally:
            self.log('info', 'Restore the lvremove binary')
            self.assertTrue(self.restore_backup_files())

            self.log('info', 'Remove the named snapshot')
            self.execute_and_wait_removesnapshot(self.ms_node,
                "-n {0}".format(snap_name))

    @attr('all', 'revert', 'story7193', 'story7193_tc18', 'kgb-physical')
    def test_18_p_delete_deployment_snap_leaves_backup_snap_intact(self):
        """
        @tms_id: litpcds_7193_tc18
        @tms_requirements_id: LITPCDS-7193
        @tms_title: Deleting deployment snapshots leaves named snapshots
        @tms_description:
            This test will verify that the user can delete deployment snapshots
            when named backup snapshots exist.
        @tms_test_steps:
            @step: Remove deployment snapshot if it exists
            @result: Deployment snapshot is removed
            @step: Create a named snapshot
            @result: Named snapshot is created
            @step: Remove named snapshot
            @result: Named snapshot has been removed
            @step: Create a deployment snapshot
            @result: Deployment snapshot has been created
            @step: Create a named snapshot
            @result: Named snapshot has been created
            @step: Remove deployment snapshot
            @result: Deployment snapshot has been removed
            @result: Named snapshot is still present
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        snap_name = "s18_01"

        self.log('info', 'Remove deployment snapshot if it exists')

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        self.log('info', 'Create named snapshot')
        self._create_named_snapshot(snap_name)

        # Store information about created snapshots
        ref_lvm_named_snaps = self.get_snapshots(self.all_nodes)
        ref_vxvm_named_snaps = self._get_vxvm_snapshots(self.mn_nodes)

        self.log('info', 'Remove named snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node,
                "--name {0}".format(snap_name))

        self.log('info', 'Create a deployment snapshot')
        self.execute_cli_createsnapshot_cmd(self.ms_node)

        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                                 test_constants.PLAN_COMPLETE,
                                                 self.timeout_mins))

        self.log('info', 'Create named snapshot')
        self._create_named_snapshot(snap_name)

        self.log('info', 'Remove deployment snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node)
        lvm_named_snaps = self.get_snapshots(self.all_nodes)
        vxvm_named_snaps = self._get_vxvm_snapshots(self.mn_nodes)

        # Verify the named snapshot is not removed
        self.assertEqual(lvm_named_snaps, ref_lvm_named_snaps)
        self.assertEqual(vxvm_named_snaps, ref_vxvm_named_snaps)

        self.execute_and_wait_removesnapshot(self.ms_node,
                "--name {0}".format(snap_name))
