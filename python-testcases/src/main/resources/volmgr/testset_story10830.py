'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     April 2015
@author:    zaarkha
@summary:   Integration
            Agile: STORY-10830
'''

from litp_generic_test import GenericTest, attr
import test_constants
from storage_utils import StorageUtils


class Story10830(GenericTest):
    """
    As a LITP architect I want snapshot plans to only use snapshot model
    context with idemopotent tasks so that I have no mco agents for task
    creation. (LVM)
    """

    def setUp(self):
        """
        Description:
            Runs before every single test.
        Actions:

            1. Call the super class setup method.
            2. Set up variables used in the tests.
        Results:
            The super class prints out diagnostics and variables
            common to all tests are available.
        """
        # 1. Call super class setup
        super(Story10830, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.node_urls = self.find(self.ms_node, "/deployments", "node")
        self.node_urls.sort()
        self.mn_nodes = self.get_managed_node_filenames()
        self.all_nodes = [self.ms_node] + self.mn_nodes
        self.storage = StorageUtils()

    def tearDown(self):
        """
        Description:
            Run after each test and performs the following:
        Actions:
            1. Cleanup after test if global results value has been used
            2. Call the superclass teardown method
        Results:
            Items used in the test are cleaned up and the
            super class prints out end test diagnostics
        """
        super(Story10830, self).tearDown()

    def create_verify_restore_verify_fill_fs(self, file_sys_dict,
                                             delete_snap,
                                             snap_name='',
                                             fs_mount_point=''):
        """
        Function to issue the create_snapshot command and the depending on
        the arguments it receives, it will either issue the remove_snapshot
        command or create a new FS and fill it. It then issues a
        restore_snapshot command It
        also verifies that the expected snapshots are created/removed. Certain
        validation tests are also executed within this function.

        Args:
            file_sys_dict (dict): Dictionary of the urls of the file systems.
            delete_snap (bool): A flag that when True will delete one of the
                                snapshots on one of the nodes and otherwise
                                will fill a file_system
            snap_name (str): The user defined snapshot name that is included
                             in to the snapshot images.
            fs_mount_point (str): Path to file system mount point.
        """
        self.log('info', 'Create a snapshot')
        self.execute_and_wait_createsnapshot(self.ms_node)
        snapshots_pre_remove = self.get_snapshots(self.all_nodes)

        if delete_snap:
            # TC_03
            self.log('info', "Manually delete a snapshot on a node")
            lv_snap_name = \
                self.build_and_run_lvremove_on_node(file_sys_dict, snap_name)
            self.log('info', 'run restore_snapshot')
            self.restore_snapshot_verify_fail(
                check_presence_fails=True, restore_snapshot_f=False,
                snap_name=lv_snap_name)

            # Verify other snapshots are not affected
            snapshots_post_remove = self.get_snapshots(self.all_nodes)
            if len(snapshots_pre_remove) == len(snapshots_post_remove) + 1:
                for snapshot in snapshots_post_remove:
                    self.assertTrue(snapshot in snapshots_pre_remove)
            else:
                self.assertTrue(False)
        else:
            # TC_04
            self.log('info', "Fill the filesystem")
            # Fill new FS
            for vol in file_sys_dict:
                if vol['type'] == "xfs":
                    node = vol['node_name']

                    fill_fs_with_yes_cmd = \
                        "/usr/bin/yes abcdefghijklmnopqrstuvwxyz " \
                        "> {0}/overload.txt".format(fs_mount_point)
                    _, _, returnc = \
                    self.run_command(node, fill_fs_with_yes_cmd,
                                     add_to_cleanup=False, su_root=True)
                    self.assertEqual(1, returnc)
                    # RESTORE
                    self.restore_snapshot_verify_fail(
                        check_presence_fails=False, restore_snapshot_f=False)

            _, _, returnc = self.restore_snapshot_verify_fail(
                check_presence_fails=False, restore_snapshot_f=True)
            self.assertEqual(0, returnc)

    def restore_snapshot_verify_fail(self, check_presence_fails,
                                     restore_snapshot_f=False,
                                     snap_name=""):
        """
        Description:
            Function to issue the restore_snapshot command and verify that
            the expected snapshots are removed.
        Args:
             check_presence_fails (bool): Flag to determine whether to check
                                          that the task to check for the
                                          presence of all snapshots fails or to
                                          check the task to check the
                                          validity of the snapshots fails
             restore_snapshot_f (bool): Flag to check whether to set the -f
                                        option
             snap_name(string): name of snapshot to look for
        Returns:
            list, list, int. std_out, std_err, rc from running show_plan
            command
        """
        args = ''
        if restore_snapshot_f:
            args = ' -f'

        # RESTORE
        restore_snapshot_cmd = self.cli.get_restore_snapshot_cmd(args)
        stdout, stderr, returnc = self.run_command(self.ms_node,
                                                   restore_snapshot_cmd,
                                                   add_to_cleanup=False,
                                                   default_asserts=True)
        if check_presence_fails:
            # CHECK TASK TO CHECK FOR PRESENCE FAILS
            self.assertTrue(
                self.wait_for_log_msg(self.ms_node,
                    "Snapshot {0} is missing on node".format(snap_name)))

        else:
            # CHECK TASK TO CHECK FOR VALIDITY FAILS
            self.assertTrue(
                self.wait_for_log_msg(self.ms_node,
                    "ERROR: Unreachable node(s):"))

            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                                 test_constants.PLAN_FAILED))
        stdout, stderr, returnc = \
        self.execute_cli_showplan_cmd(self.ms_node)

        return stdout, stderr, returnc

    def build_and_run_lvremove_on_node(self, file_sys_dict, snap_name):
        """
        Description:
            Function to issue the lvremove command on a node.
        Args:
            file_sys_dict (dict): A dictionary of all the file systems below
                                  each node.
            snap_name (str): The user defined snapshot name that is included
                             in to the snapshot images.
        Return:
           compiled snap name
        """
        for vol in file_sys_dict:
            if vol['type'] == "xfs":
                url = vol['path']
                vol_grp_name = vol['volume_group_name']
                snap_name = self.get_snapshot_name_from_url(url,
                        name=snap_name)
                node = vol['node_name']
                lvsnap_location = "/dev/{0}/{1}".format(vol_grp_name,
                        snap_name)
                lvremove_cmd = self.storage.get_lvremove_cmd(lvsnap_location,
                        '-f')

                self.run_command(node, lvremove_cmd, add_to_cleanup=False,
                                 su_root=True, default_asserts=True)
                return snap_name

    def add_ms_to_file_sys_dict(self, file_sys_dict):
        """
        Description:
            Adds the kickstart created file systems to the file_sys_dict
            in the means of forged urls to fit the other functions.
        Args:
            file_sys_dict (dict): Dictionary of all the file systems.
        Returns:
            dict.
        """
        path_list = ["/deployments/d1/clusters/c1/nodes/ms1/storage_profile" \
             "/volume_groups/lv/file_systems/root",
             "/deployments/d1/clusters/c1/nodes/ms1/storage_profile" \
             "/volume_groups/lv/file_systems/home",
             "/deployments/d1/clusters/c1/nodes/ms1/storage_profile" \
             "/volume_groups/lv/file_systems/var"]

        for path in path_list:
            file_sys_dict.append(
                {'storage_profile': 'dummy', 'mount_point': '/',
                'vg_item_id': 'dummy', 'volume_driver': 'lvm',
                'volume_group_name': 'dummy', 'volume_name': 'root',
                'snap_size': '100', 'path':
                path, 'type': 'xfs', 'size': 'dummy',
                'node_name': str(self.ms_node)}
                )

        return file_sys_dict

    @staticmethod
    def get_snapshot_name_from_url(url, vol_driver='lvm', name=''):
        """
        Description:
            Function to compile the snapshot name of the file-system at
            the provided url.

        Args:
            url (str): url to a file system object.
            vol_driver (str): The volume driver under test, lbm or vxvm.
            name (str): user specified name to include in snapshots.

        Returns:
            The snapshot name.
        """
        str_list = url.split('/')
        index = str_list.index('volume_groups')
        fs_id = str_list[index + 3]
        if vol_driver == 'lvm':
            vg_id = str_list[index + 1]
            if name != '':
                return "L_{0}_{1}_{2}".format(vg_id, fs_id, name)

            return "L_{0}_{1}_".format(vg_id, fs_id)

        else:
            if name != '':
                return "L_{0}_{1}".format(fs_id, name)

            return "L_{0}_".format(fs_id)

    @attr('all', 'revert', 'story10830', 'story10830_tc03')
    def test_03_n_restore_snap_presence_chk_fail_when_snap_missing(self):
        '''
            @tms_id: litpcds_10830_tc03
            @tms_requirements_id: LITPCDS-10830
            @tms_title: Snapshot presence task fails if snapshots are missing
            @tms_description:
                To ensure that should a snapshot created by LITP be manually
                removed, that on issuing the restore_snapshot command that the
                task to check for the presence of all expected snapshots fails.
            @tms_test_steps:
                @step: Create a snapshot
                @result: Snapshots are created
                @step: Manually delete a snapshot on a node
                @result: Snapshot is deleted
                @step: run restore_snapshot
                @result: Plan fails
                @result: Task to check for the presence of the snapshot fails
                @result: There is an error in the logs
                @result: All other snapshots are still available
                @step: run restore_snapshot -f
            @result: Restore completes

            @tms_test_precondition: NA
            @tms_execution_type: Automated
        '''

        self.remove_all_snapshots(self.ms_node)

        file_sys_dict = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        self.create_verify_restore_verify_fill_fs(file_sys_dict, True)

        self.log('info', 'Run restore_snapshot -f')
        self.execute_and_wait_restore_snapshot(self.ms_node, args="-f")

    def verify_snaps_created(self, file_sys_dict, snap_name=""):
        """
        Description:
            Ensures that the provided file systems have indeed
            been snapshot on the nodes.
        Args:
            file_sys_dict (dict): Identifies the snapshots
            snap_name (str): Any specific name tag assigned.
        """
        for vol in file_sys_dict:
            if vol['type'] == "xfs":
                compiled_snap_name = self.get_snapshot_name_from_url(
                        vol['path'], 'lvm', snap_name)

                node = vol['node_name']
                self.verify_snaps_on_node(node, compiled_snap_name)

    def verify_snaps_on_node(self, node, snap_name):
        """
        Description:
            Verifies that the list of snapshots
            have indeed been created on the specified node.
        Args:
            node (str): The node on which the check is to be
                        performed.
            snap_name (str): The snap for which to check.
        """
        lvscan_cmd = self.storage.get_lvscan_cmd()
        stdout, _, _ = self.run_command(node, lvscan_cmd, su_root=True)
        combined_lvscan_output = \
                "".join(stdout).replace(' ', '').replace('\t',
                                                         '').replace('\n', '')
        self.assertTrue(
                "ACTIVESnapshot'/dev/vg_root/{0}'".format(snap_name)
                in combined_lvscan_output,
                "ACTIVESnapshot'/dev/vg_root/{0}'".format(snap_name))

    @attr('all', 'revert', 'story10830', 'story10830_tc07')
    def test_07_n_restore_snap_presence_chk_fail_when_node_unreach(self):
        '''
        @tms_id: litpcds_10830_tc07
        @tms_requirements_id: LITPCDS-10830
        @tms_title: Snapshot presence task fails if node is down
        @tms_description:
            To ensure that should a node, on which resides snapshots
            created by LITP, be powered off, that on issuing the
            restore_snapshot command that the task to check for
            the presence of all expected snapshots fails.
        @tms_test_steps:
            @step: Create a snapshot
            @result: Snapshots are created
            @step: Shutdown a node
            @result: Node is down
            @step: run restore_snapshot
            @result: Plan fails
            @result: There is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)
        file_sys_dict = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        try:
            self.log('info', 'Create a snapshot')
            self.execute_and_wait_createsnapshot(self.ms_node)
            file_sys_dict = self.add_ms_to_file_sys_dict(file_sys_dict)
            self.verify_snaps_created(file_sys_dict)

            self.log('info', 'Shutdown a node')
            self.poweroff_peer_node(self.ms_node, self.mn_nodes[0])
            self.disconnect_all_nodes()

            self.log('info', 'Run restore_snapshot')
            self.restore_snapshot_verify_fail(False)

            self.execute_and_wait_restore_snapshot(self.ms_node, args="-f")
            self.poweron_peer_node(self.ms_node, self.mn_nodes[0])
        finally:
            self.poweron_peer_node(self.ms_node, self.mn_nodes[0])

    @attr('all', 'revert', 'story10830', 'story10830_tc09')
    def test_09_p_node_down_remove_snap_force_succ(self):
        '''
        @tms_id: litpcds_10830_tc09
        @tms_requirements_id: LITPCDS-10830
        @tms_title: Snapshot presence task fails if node is down
        @tms_description:
             To ensure that in the case where a node on which a snapshot/named
             snapshot taken by LITP resides is no longer reachable that the
             remove_snapshot command shall fail and require the usage of the
             -f flag to be successful.
        @tms_test_steps:
            @step: Create a deployment snapshot
            @result: Deployment snapshot is created
            @step: Shutdown a node
            @result: Node is down
            @step: Issue remove_snashot
            @result: Remove snapshot fails
            @step: Issue remove_snapshot -f
            @result: Plan succeeds
            @result: There is a note in the logs regarding the unreachable node
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        log_path = test_constants.GEN_SYSTEM_LOG_PATH
        log_len = self.get_file_len(self.ms_node, log_path)

        self.log('info', 'Create a deployment snapshot')
        self.execute_and_wait_createsnapshot(self.ms_node)

        self.log("info", "Shutdown a node")
        self.poweroff_peer_node(self.ms_node, self.mn_nodes[0])

        self.log("info", "Issue remove_snapshot command")
        self.execute_cli_removesnapshot_cmd(self.ms_node)
        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                         test_constants.PLAN_FAILED))

        self.log("info", "Issue remove_snapshot -f command")
        self.execute_and_wait_removesnapshot(self.ms_node,
                args=" -f")

        # Verify log message
        log_msg = "Node \\\"{0}\\\" not currently " \
                   "reachable. Continuing.".format(self.mn_nodes[0])
        self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg,
            log_len=log_len))

        self.poweron_peer_node(self.ms_node, self.mn_nodes[0])
        self.manual_snap_clean(self.mn_nodes[0])

    def manual_snap_clean(self, node):
        """
        Description:
            Function to manually remove snaps
        Args:
            node (str): filename of the node on which to exe
        """
        lvscan_cmd = self.storage.get_lvscan_cmd()
        stdout, _, _ = \
        self.run_command(node, lvscan_cmd, su_root=True)
        for item in stdout:
            if 'Snapshot' in item:
                lvsnap_location = item.split("'")[1]
                lvremove_cmd = \
                self.storage.get_lvremove_cmd(lvsnap_location,
                                              '-f')
                self.run_command(node, lvremove_cmd, add_to_cleanup=False,
                                 su_root=True)

    @attr('all', 'revert', 'story10830', 'story10830_tc12')
    def test_12_p_remove_snapshot_fails_on_merging_snapshots(self):
        """
        @tms_id: litpcds_10830_tc12
        @tms_requirements_id: LITPCDS-10830
        @tms_title: Remove snapshot fails fails on merging snapshots
        @tms_description:
            This test will verify that when a user runs remove_snapshot, the
            plan will fail if any merging snapshot are present.
        @tms_test_steps:
            @step: Create a snapshot of all nodes.
            @result: Snapshot plan suceeds
            @step: Set snapshots in MN vgs, MS vg_root, MS non-vg_root to a
                   merging state
            @result: snapshots are in a merging state
            @step: Run the litp remove_snapshot command
            @result: Plans fails
            @result: there is an error in the logs
            @step: Run the litp remove_snapshot -f command
            @result: Plans fails
            @result: there is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        err_msg = "is merging, can't proceed."

        self.log('info', 'Create a snapshot of all nodes')
        self.remove_all_snapshots(self.ms_node)

        file_sys_dict = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        self.execute_and_wait_createsnapshot(self.ms_node)

        self.log('info',
              'Set one snapsnot on each of the nodes and ms to be merging')
        self.build_and_run_lvconvert_on_nodes(file_sys_dict)
        self.build_and_run_lvconvert_on_ms()

        self.log('info', 'Run the litp remove_snapshot command')
        self.execute_cli_removesnapshot_cmd(self.ms_node)

        self.log('info', 'Verify error message')
        self.assertTrue(self.wait_for_log_msg(self.ms_node, err_msg))

        self.log('info', 'Run the litp remove_snapshot -f command')
        self.execute_cli_removesnapshot_cmd(self.ms_node, args=' -f')

        self.log('info', 'Verify error message')
        self.assertTrue(self.wait_for_log_msg(self.ms_node, err_msg))

        self.log('info', 'Reboot the nodes')
        ms_hostname = self.get_props_from_url(self.ms_node, "/ms", "hostname")
        for vol in file_sys_dict:
            if vol['type'] == "xfs":
                host = vol['node_name']
                if not host == ms_hostname:
                    self.reboot_node(host)

        self.log('info', 'Reboot the ms')
        self.reboot_node(ms_hostname)
        self.execute_and_wait_restore_snapshot(self.ms_node, skip_cmd=True)

    def build_and_run_lvconvert_on_nodes(self, file_sys_dict, snap_name=''):
        """
        Description:
            Function to issue the lvconvert command on one snapshot on each
            node to simulate a merging snapshot for testing purposes.
        Args:
            file_sys_dict (dict): A dictionary of all the file systems below
                                  each node.
            snap_name (str): The user defined snapshot name that is included
                             in the snapshot images. Default is ''
        """
        # for the ms and each of the nodes
        for vol in file_sys_dict:
            if vol['type'] == "xfs":
                url = vol['path']
                full_snap_name = self.get_snapshot_name_from_url(url,
                                     name=snap_name)

                # Make sure the chosen volume isn't the root directory
                if full_snap_name == "L_lv_root_":
                    full_snap_name = "L_lv_home_"

                host = vol['node_name']
                lvsnap_loc = "/dev/{0}/{1}".format(
                        vol['volume_group_name'], full_snap_name)
                lvmerge_cmd = "/sbin/lvconvert --merge {0}".format(lvsnap_loc)
                _, _, returnc = \
                self.run_command(host, lvmerge_cmd, add_to_cleanup=False,
                                 su_root=True)
                self.assertEqual(0, returnc)

    def build_and_run_lvconvert_on_ms(self):
        """
        Description:
            Function to issue the lvconvert command on one snapshot on
            the ms
        """
        # for the ms and each of the nodes
        full_snap_name = "L_lv_var_"

        # Make sure the chosen volume isn't the root directory
        lvsnap_loc = "/dev/{0}/{1}".format("vg_root", full_snap_name)
        lvmerge_cmd = "/sbin/lvconvert --merge {0}".format(lvsnap_loc)
        _, _, returnc = \
            self.run_command(self.ms_node, lvmerge_cmd, add_to_cleanup=False,
                                                               su_root=True)
        self.assertEqual(0, returnc)

    def reboot_node(self, node, su_root=True):
        """ Reboot a node and wait for it to come up. """
        cmd = "/sbin/reboot"
        self.run_command(node, cmd, su_root=su_root)
