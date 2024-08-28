'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     April 2015
@author:    Philip Daly, Dara mcHugh, Carlos Branco
@summary:   Integration
            Agile: STORY-10831
'''

from litp_generic_test import GenericTest, attr
import test_constants
import time
import os
from redhat_cmd_utils import RHCmdUtils
import re


class Story10831(GenericTest):
    """
    As a LITP architect I want snapshot plans to use
    snapshot model context with idempotent tasks so
    that I have no mco agents for task creation on
    peer nodes (VXVM)
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
        super(Story10831, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.node_urls = self.find(self.ms_node, "/deployments", "node")
        self.node_urls.sort()
        self.mn_nodes = self.get_managed_node_filenames()
        # Current assumption is that only 1 VCS cluster will exist
        self.vcs_cluster_url = self.find(self.ms_node,
                                         "/deployments", "vcs-cluster")[-1]
        self.rpm_src_dir = \
            os.path.dirname(os.path.realpath(__file__)) + \
            "/test_lsb_rpms/"
        # Repo where rpms will be installed
        self.repo_dir_3pp = test_constants.PP_PKG_REPO_DIR
        self.rh_cmds = RHCmdUtils()

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
        super(Story10831, self).tearDown()

    def retrieve_file_system_dict(self, vol_driver='lvm'):
        """
        Description:
            Function to retrieve a dictionary of the file systems which
            reside below each node.
        Args:
            vol_driver (str): The volume driver under test, lvm or vxvm.
        Returns:
            dict. A dictionary of all the file systems below each node.
        """
        storage_profiles = self.get_storage_profile_paths(self.ms_node,
                                                          vol_driver,
                                                          "/deployments")
        all_file_systems = []
        file_sys_dict = {}
        for storage_profile in storage_profiles:
            volume_groups = \
            self.get_all_vol_grps_from_storage_profile(storage_profile)
            for volume_group in volume_groups:
                all_file_systems.extend(self.get_all_file_sys_from_vol_grp(
                                        volume_group))
        for file_sys in all_file_systems:
            file_sys_dict = \
            self.compile_dict(file_sys, file_sys_dict, vol_driver)
        return file_sys_dict

    def get_all_vol_grps_from_storage_profile(self, url):
        """
        Description:
            Function to retrieve all of the volume group object residing
            below the provided storage profile url.
        Args:
            url (str): A url to a storage profile object.
        Returns:
            list. A list of all of the volume group objects found.
        """
        stdout = self.find(self.ms_node, url, "volume-group")
        return stdout

    def compile_dict(self, url, fs_dict, vol_driver='lvm'):
        """
        Description:
            Function to compile a dictionary identifying the file systems
            below a node. Depending on the volume driver specified
            props shall be altered to be passed to the child function
            populate_dict.
        Args:
            url (str): A url of the node object.
            fs_dict (dict): The dictionary to be populated.
            vol_driver (str): The volume driver of the storage profile parent;
                              Could be lvm or vxvm.
        Returns:
            dict. A dictionary of all the file systems below each node.
        """
        if vol_driver == 'lvm':
            url_list = url.split('/')
            node_index = url_list.index('nodes')
            # LIST COMPREHENSION TO COMPILE NODE URL FROM CHILD URL PROVIDED
            node_url = \
            "".join(["/" + x for x in url_list[:node_index + 2] if x != ''])

            fs_dict = self.populate_dict(url, node_url, fs_dict)
        else:
            for node_url in self.node_urls:
                fs_dict = self.populate_dict(url, node_url, fs_dict)

        return fs_dict

    def populate_dict(self, fs_url, node_url, fs_dict):
        """
        Description:
            Function to populate a dictionary identifying the file systems
            below a node from properties passed from the parent function
            compile_dict.
        Args:
            fs_url (str): A url of the file system object.
            node_url (str): A url of the node object.
            dict (dict): The dictionary to be populated.
        Returns:
            dict. A dictionary of all the file systems below each node.
        """
        hostname = self.get_props_from_url(self.ms_node, node_url, 'hostname')

        if hostname not in fs_dict.keys():
            fs_dict[hostname] = {}
        stdout = \
        self.get_props_from_url(self.ms_node, fs_url)
        snap_external = 'false'
        if 'snap_external' in stdout:
            snap_external = \
            self.get_props_from_url(self.ms_node, fs_url, 'snap_external')
        if snap_external not in fs_dict[hostname].keys():
            fs_dict[hostname][snap_external] = {}
        stdout = self.get_props_from_url(self.ms_node, fs_url, 'type')
        if stdout not in fs_dict[hostname][snap_external].keys():
            fs_dict[hostname][snap_external][stdout] = []
        fs_dict[hostname][snap_external][stdout].append(fs_url)
        return fs_dict

    def get_all_file_sys_from_vol_grp(self, url):
        """
        Description:
            Function to retrieve all of the file system objects residing
            below the provided volume group object.
        Args:
            url (str): A url of the volume group object.
        Returns:
            list. A list of all the file system objects found.
        """
        stdout = self.find(self.ms_node, url, "file-system")
        return stdout

    def chk_restore_plan_tasks(self, stdout, args="", lvm_tasks=False):
        """
        DEscription:
            Function to ensure that the specified tasks
            are either present or absent from the restore
            plan - dependent upon whether the force
            argument is specified.
        Args:
            stdout (list): output of the show_plan cmd
            args (str): Argument specified with restore
            lvm_tasks (bool): Defines whether lvm snaps should be
                              present.
        """
        combined_plan_output = self.combine_plan_output(stdout)

        self.assertTrue("CheckVxVMsnapshotsarevalid"
                        in combined_plan_output)
        if lvm_tasks == True:

            self.assertTrue('CheckLVMsnapshotsonnode(s)'
                            in combined_plan_output)

            self.assertTrue('arevalid'
                            in combined_plan_output)

        if '-f' in args:
            self.assertFalse("Checkthatanactivenode" \
                             "existsforeachVxVMvolumegroup"
                             in combined_plan_output)
            self.assertFalse("CheckVxVMsnapshotsarepresent"
                             in combined_plan_output)
            if lvm_tasks == True:
                self.assertFalse('Checkpeernode(s)'
                                 in combined_plan_output)
                self.assertFalse('withallLVMsnapshotspresent'
                                 in combined_plan_output)
                self.assertTrue('Restartandwaitfornodes'
                                in combined_plan_output)
        else:

            self.assertTrue("Checkthatallnodesarereachable" \
                            "andanactivenodeexistsforeachVxVMvolumegroup"
                            in combined_plan_output)

            self.assertTrue("CheckVxVMsnapshotsarepresent"
                            in combined_plan_output)
            if lvm_tasks == True:

                self.assertTrue('Checkpeernode(s)'
                                in combined_plan_output)

                self.assertTrue('withallLVMsnapshotspresent'
                                in combined_plan_output)

                self.assertTrue('Restartnode(s)'
                                in combined_plan_output)

                for node in self.mn_nodes:
                    wait_for_node_text = \
                    'Waitfornode"{0}"torestart'.format(node)
                    self.assertTrue(wait_for_node_text
                                    in combined_plan_output)

        self.assertTrue("RestoreVxVMdeploymentsnapshot"
                        in combined_plan_output)

    @staticmethod
    def combine_plan_output(stdout):
        """
        Description:
            Removes all space, tab, and newline
            characters from the plan output.
        Args:
            stdout (list): plan output
        Returns:
            str. The combined plan output.
        """
        return \
        "".join(stdout).replace(' ',
                                '').replace('\t',
                                            '').replace('\n',
                                                        '')

    @staticmethod
    def get_snapshot_name_from_url(url, vol_driver='lvm', name=''):
        """
        Description:
            Function to compile the snapshot name of the file-system at
            the provided url.

        Args:
            url (str): url to a file system object.
            vol_driver (str): The volume driver under test, lvm or vxvm.
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

    def compile_list_of_file_sys(self, file_sys_dict, fs_type='ext4',
                                      search_type='false'):
        """
        Description:
            Function to compile a list of the specified file system types
            residing below the file systems specified.
        Args:
            file_sys_dict (dict): Dictionary of the urls of the file systems.
            fs_type (str): The type of file system to be searched for.
            search_type (str): the dictionary key under which to retrieve all
                               found file systems of the specified type.
        Returns:
            list. dict. A list of the file system objects and a dictionary
                        of the cleanup tasks to be executed.
        """
        type_url_list = []
        cleanup_dict = {}
        for node in file_sys_dict.keys():
            external_settings = file_sys_dict[node].keys()
            if search_type not in external_settings:
                continue
            file_types = file_sys_dict[node][search_type].keys()
            if fs_type in file_types:
                type_urls = file_sys_dict[node][search_type][fs_type]
                type_url_list.extend(type_urls)
                for type_url in type_urls:
                    stdout, _, _ = \
                    self.execute_cli_show_cmd(self.ms_node, type_url)
                    if self.is_text_in_list("snap_external", stdout):
                        cleanup_dict[type_url] = search_type
                    else:
                        cleanup_dict[type_url] = None
        return type_url_list, cleanup_dict

    def cleanup_updates(self, cleanup_dict):
        """
        Description:
            Function to clean up the updates made to the file system
            objects so as to return them to the state in which they began.
        Args:
            cleanup_dict (dict): Dictionary identifying the file system
                                 objects that were updated and the original
                                 property to which they need be restored.
        """
        for url in cleanup_dict.keys():
            self.execute_cli_update_cmd(self.ms_node, url,
                                            props='snap_external',
                                            action_del=True)

    def _disable_lvm_snapshots(self):
        """
        disables peer node lvm snapshots by setting snap_external to true
        """
        fss = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        # set snap_external to true to avoid creation of task
        # 'Check peer nodes "node2" and "node1" are reachable'
        for fsystem in fss:
            self.backup_path_props(self.ms_node, fsystem['path'])
            self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                    props='snap_external=true')

    @attr('all', 'revert', 'story10831', 'story10831_tc05', 'kgb-physical')
    def test_05_p_remove_snapshot_node_shutdown_success(self):
        '''
        @tms_id: litpcds_10831_tc05
        @tms_requirements_id: LITPCDS-10831
        @tms_title: remove snapshot one node shutdown
        @tms_description:
            To ensure that should a snapshot remove_snapshot be
            issued while one of the nodes in the vcs cluster be
            shutdown that the plan is successful.
        @tms_test_steps:
            @step: Create a snapshot
            @result: Plan succeeds
            @result: no peer node LVM snapshots are taken (disabled previously)
            @result: vxvm snapshot exists
            @step: Shutdown the vxvm node
            @result: vxvm node is shut down
            @step: Run remove_snapshot
            @result: Plan succeeds
            @result: vxvm snapshot is removed
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        ilo_ipadd = None
        fss_vxvm = self.get_all_volumes(self.ms_node)
        node = self.get_vx_disk_node(self.ms_node,
        disk_group=fss_vxvm[0]['volume_group_name'])
        cmd = self.sto.get_vxsnap_cmd(fss_vxvm[0]['volume_group_name'],
                grep_args="L_")
        try:
            self._disable_lvm_snapshots()
            self.log('info', 'Create a snapshot')
            if not self.is_snapshot_item_present(self.ms_node):
                self.execute_and_wait_createsnapshot(self.ms_node)

            self.log('info', 'Verify vxvm snapshot was created')
            self.run_command(node, cmd, su_root=True, default_asserts=True)

            self.log("info", "Shutdown the vxvm node")
            ilo_ipadd = self.get_node_ilo_ip(self.ms_node, node)
            self.poweroff_peer_node(self.ms_node, node, ilo_ip=ilo_ipadd)

            self.log("info", "Run remove_snapshot")
            self.execute_and_wait_removesnapshot(self.ms_node)

            self.log('info', 'Verify vxvm snapshot was removed')
            for peer_node in self.mn_nodes:
                if peer_node != node:
                    _, _, ret_code = self.run_command(peer_node, cmd,
                            su_root=True)
                    self.assertEqual(1, ret_code)

        finally:
            self.poweron_peer_node(self.ms_node, node,
                                   ilo_ip=ilo_ipadd)

    def chk_and_update_snap_external(self, file_sys_dict):
        """
        Description:
            Checks that some F-S with the correct
            snap external values exists, if not it sets them
        Args:
            file_sys_dict (dict): a dict of all the vxvm file sys
        Returns:
            dict, dict.
        """
        cleanup_dict = {}
        prop_found = False
        for node in file_sys_dict.keys():
            external_settings = file_sys_dict[node].keys()
            if 'false' in external_settings:
                if 'vxfs' in file_sys_dict[node]['false'].keys():
                    prop_found = True
                    break
            else:
                continue

        # IF NOT THEN UPDATE AN VXFS TO BE FALSE
        # IF NO VXFS EXISTS THEN RAISE AN EXCEPTION.
        if not prop_found:
            vxfs_url_list, cleanup_dict = \
            self.compile_list_of_file_sys(file_sys_dict, fs_type='vxfs',
                                          search_type='true')
            self.assertNotEqual([], vxfs_url_list)
            for url in vxfs_url_list:
                self.execute_cli_update_cmd(self.ms_node, url,
                                            props='snap_external=false')
            file_sys_dict = self.retrieve_file_system_dict("vxvm")
        return file_sys_dict, cleanup_dict

    @attr('all', 'revert', 'story10831', 'story10831_tc06', 'kgb-physical')
    def test_06_n_remove_snapshot_node_shutdown_fail(self):
        """
        @tms_id: litpcds_10831_tc06
        @tms_requirements_id: LITPCDS-10831
        @tms_title: remove snapshot all node shutdown
        @tms_description:
            To ensure that should a snapshot remove_snapshot be
            issued while all of the nodes in the vcs cluster be
            shutdown that the plan fails.
        @tms_test_steps:
            @step: If llthosts fact is not locally synchronized, run Puppet
            @result: Fact is available
            @step: Update the vcs_seed_threshold property to 2
            @result: Item updated
            @step: Create a Plan
            @result: Plan created
            @step: Run a Plan
            @result: Plan succeeds
            @step: Create a snapshot
            @result: Plan succeeds
            @result: no peer node LVM snapshots are taken (disabled previously)
            @result: vxvm snapshot exists
            @step: Shutdown all nodes
            @result: vxvm node is shut down
            @step: Run remove_snapshot
            @result: Plan fails
            @result: There is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        mn_node_hostname = self.mn_nodes[0]
        fact_name = 'llthosts'

        if not self.is_puppet_synched(
                                mn_node_hostname, self.ms_node, fact_name):
            self.log('info',
                     ('The "{0}" fact for node "{1}" is not available to the '
                      'Puppet server. Triggering a Puppet run and waiting '
                      'until it completes').format(fact_name,
                                                   mn_node_hostname))
            self.wait_full_puppet_run(self.ms_node)

        # updating the property of vcs_seed_threshold to 2
        # as part of TORF-186950
        timeout = 90
        self.log("info", "Updating the value of the vcs_seed_threshold")
        self.execute_cli_update_cmd(self.ms_node,
                                    self.vcs_cluster_url,
                                    props='vcs_seed_threshold=2'
                                    )

        # Execute the plan
        self.execute_cli_createplan_cmd(self.ms_node)
        self.execute_cli_showplan_cmd(self.ms_node)
        self.execute_cli_runplan_cmd(self.ms_node)

        self.wait_for_plan_state(self.ms_node,
                                 test_constants.PLAN_COMPLETE,
                                 timeout_mins=timeout)

        # Debug output for TORF-143213
        cmd = "/opt/VRTS/bin/vxfenadm -s all -f /etc/vxfentab"
        for node in self.mn_nodes:
            self.run_command(node, cmd, su_root=True)

        self.remove_all_snapshots(self.ms_node)
        self._disable_lvm_snapshots()

        try:
            self.log('info', 'Create a snapshot')
            self.execute_and_wait_createsnapshot(self.ms_node)

            self.log('info', 'Shutdown all nodes')
            for node in self.mn_nodes:
                ilo_ipadd = self.get_node_ilo_ip(self.ms_node, node)
                self.poweroff_peer_node(self.ms_node, node,
                                        ilo_ip=ilo_ipadd)

            self.log("info", "Run remove_snapshot")
            self.execute_cli_removesnapshot_cmd(self.ms_node)
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                            test_constants.PLAN_FAILED))
            self.assertEqual(test_constants.PLAN_TASKS_FAILED,
                            self.get_task_state(self.ms_node,
                "Check that an active node exists for each VxVM volume"))

        finally:
            # RESTART THE NODES
            for node in self.mn_nodes:
                ilo_ipadd = self.get_node_ilo_ip(self.ms_node, node)
                self.poweron_peer_node(self.ms_node, node,
                                       ilo_ip=ilo_ipadd)

            vx_disk_node = None
            counter = 0
            # wait vxvm to be up
            while not vx_disk_node and counter < 9:
                try:
                    vx_disk_node = self.get_vx_disk_node(self.ms_node)
                except AssertionError:
                    pass
                time.sleep(10)
                counter += 1

        # deleting the property of vcs_seed_threshold
        # as part of TORF-186950
        self.log("info",
                         "Deleting the value of the vcs_seed_threshold")
        self.execute_cli_update_cmd(self.ms_node,
                                    self.vcs_cluster_url,
                                    "vcs_seed_threshold",
                                    action_del=True)

        # Execute the plan
        self.execute_cli_createplan_cmd(self.ms_node)
        self.execute_cli_showplan_cmd(self.ms_node)
        self.execute_cli_runplan_cmd(self.ms_node)

        self.wait_for_plan_state(self.ms_node,
                                 test_constants.PLAN_COMPLETE,
                                 timeout_mins=timeout)

    def get_vxprint_console_output(self, node):
        """
        Description:
            Function to get the console output of vxprint.
        Args:
            node (str): Filename of the node
        Returns:
            str. The vxprint -vt console output.
        """
        stdout, _, _ = \
        self.run_command(node,
                         self.get_vxprint_cmd("-vt"), su_root=True)
        return stdout

    @attr('all', 'revert', 'story10831', 'story10831_tc08', 'kgb-physical')
    def test_08_n_restore_snap_pres_chk_fail_when_snap_miss(self):
        """
        @tms_id: litpcds_10831_tc08
        @tms_requirements_id: LITPCDS-10831
        @tms_title: remove snapshot all node shutdown
        @tms_description:
            To ensure that should a snapshot created by LITP be
            manually removed, that on issuing the restore_snapshot
            command that the task to check for the presence
            of all expected snapshots fails.
        @tms_test_steps:
            @step: Create a snapshot
            @result: Plan succeeds
            @step: Manually delete the vxvm snapshot
            @result: vxvm snapshot is deleted
            @step: Run restore_snapshot
            @result: Plan fails
            @result: Task for snapshot presents fails
            @result: There is an error in the logs
            @step: Run restore_snapshot -f
            @result: System is restored

        @tms_test_precondition: NA
        @tms_execution_type: Automated

        """

        # Debug output for TORF-143213
        cmd = "/opt/VRTS/bin/vxfenadm -s all -f /etc/vxfentab"
        for node in self.mn_nodes:
            self.run_command(node, cmd, su_root=True)

        self.log("info", "Create a snapshot")
        if not self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_createsnapshot(self.ms_node)

        self.log("info", "Manually delete the vxvm snapshot")
        self.manually_remove_snap()

        self.log("info", "Run restore snapshot")
        self.execute_cli_restoresnapshot_cmd(self.ms_node)
        self.assertTrue(self.wait_for_log_msg(self.ms_node,
            "Snapshot(s) missing: Volume Group"))

        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                        test_constants.PLAN_FAILED))

        self.assertEqual(test_constants.PLAN_TASKS_FAILED,
            self.get_task_state(self.ms_node,
                "Check VxVM snapshots are present"))

        self.log("info", "Run restore snapshot -f")
        self.execute_and_wait_restore_snapshot(self.ms_node, args="-f")

    def verify_vxsnap_present(self, file_sys_url, chk_for_absence=False):
        """
        Description:
            Function to verify that the expected vxvm snapshots
            are present on the nodes.
        Args:
            file_sys_url (str): The url to the vxvm fs
            chk_for_absence (bool): Flag to state whether to check
                                    snap exists or not.
        """
        snapshot_name = \
        self.get_snapshot_name_from_url(file_sys_url, 'vxvm')
        vol_grp_url = self.get_vol_grp_from_vxfs_fs_url(file_sys_url)
        vol_grp_id = self.get_vol_grp_id_from_url(vol_grp_url)
        active_node = self.get_active_node_for_vol_grp(vol_grp_id)
        vxprint_output = self.get_vxprint_console_output(active_node)
        combined_vxp_output = \
        self.combine_plan_output(vxprint_output)
        if not chk_for_absence:
            self.assertTrue(snapshot_name in combined_vxp_output)
        else:
            self.assertFalse(snapshot_name in combined_vxp_output)

    @attr('manual-test', 'non-revert', 'story10831',
          'story10831_tc12', 'kgb-physical')
    def test_12_n_restore_snapshot_validity_chk_fail(self):
        """
        Description:
            To ensure that should a snapshot which was created by
            LITP be corrupt on a node that the restore_snapshot
            task to check the validity of the snapshots fails
            even when issued along with the force argument.
        Actions:
            1. Ascertain which vxvm file systems should have snapshots
               created - that is those with snap_external=false,
               snap_size not = 0, and not of type swap.
            2. Create and deploy a new file-system
            3. Issue the create_snapshot command.
            4. Verify that the snapshots have been created on the nodes,
               use vxprint -vt.
            5. Corrupt the snapshot of the new file-system.
            6. Issue the restore_snapshotcommand.
            7. Check the plan tasks
            8. Ensure the plan fails at the vailidty check task.
        """
        self.remove_all_snapshots(self.ms_node)
        file_sys_dict = self.retrieve_file_system_dict('vxvm')
        cleanup_dict = {}
        mount_point = ""
        active_node = ""
        self.log("info", "Starting action 1")
        # ASCERTAIN WHETHER AN VXFS WITH SNAP EXTERNAL FALSE EXISTS.
        file_sys_dict, cleanup_dict = \
        self.chk_and_update_snap_external(file_sys_dict)

        try:
            self.log("info", "Starting action 2")
            vg_id = "vg_10831"
            mount_point = "vxvm10831"
            vg_id, mount_point, _, already_exists = \
            self.create_new_file_system(file_sys_dict)
            if not already_exists:
                self.execute_cli_createplan_cmd(self.ms_node)
                self.execute_cli_runplan_cmd(self.ms_node)
                self.wait_for_plan_state(self.ms_node,
                                         test_constants.PLAN_COMPLETE)

            self.log("info", "Starting action 3")
            self.execute_cli_createsnapshot_cmd(self.ms_node)
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                            test_constants.PLAN_COMPLETE))

            self.log("info", "Starting action 4")
            nodes = file_sys_dict.keys()
            node = nodes[0]
            file_sys_urls = file_sys_dict[node]['false']['vxfs']
            for file_sys_url in file_sys_urls:
                self.verify_vxsnap_present(file_sys_url)

            self.log("info", "Starting action 5")
            active_node = self.get_active_node_for_vol_grp(vg_id)
            self.corrupt_snap_on_node(active_node, mount_point)
            counter = 0
            snap_invalidated = False
            # YOU MUST WAIT A FEW SECONDS FOR VCS TO IDENTIFY THE
            # INVALIDATED SNAP AND FLAG IT AS AN ERROR.
            while counter < 12 and snap_invalidated == False:
                counter, snap_invalidated = \
                self.chk_snap_validity(active_node, counter,
                                       snap_invalidated)
            self.assertTrue(snap_invalidated)

            self.log("info", "Starting action 6")
            self.execute_cli_restoresnapshot_cmd(self.ms_node, args="-f")

            self.log("info", "Starting action 8")
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                            test_constants.PLAN_FAILED))
            stdout, _, _ = \
            self.execute_cli_showplan_cmd(self.ms_node)
            failed_task = \
            "Failed/snapshots/snapshotCheckVxVMsnapshotsarevalid"
            combined_plan_output = self.combine_plan_output(stdout)
            self.assertTrue(failed_task in combined_plan_output)

            self.log("info", "Starting action 7")
            self.chk_restore_plan_tasks(stdout, args="-f",
                                        lvm_tasks=True)

        finally:
            # CLEANUP ANY UPDATES
            if cleanup_dict != {}:
                self.cleanup_updates(cleanup_dict)
            self.execute_cli_removesnapshot_cmd(self.ms_node)
            self.wait_for_plan_state(self.ms_node,
                                     test_constants.PLAN_COMPLETE)

    def chk_snap_validity(self, node, counter, snap_invalidated):
        """
        Description:
            Checks for an invalid statement appearing
            in the vcs console
        Args:
            node (str): node on which the cmd is to be exe
            counter (int): A counter for an enclosing loop.
            snap_invalidated (bool): Flag for if found.
        Returns:
            int, bool.
        """
        cmd = self.get_vxprint_cmd("-vt")
        stdout, _, _ =\
        self.run_command(node, cmd, su_root=True)
        combined_plan_output = self.combine_plan_output(stdout)
        if "DETACHEDINVALID" in combined_plan_output:
            snap_invalidated = True
            return counter, snap_invalidated
        else:
            time.sleep(5)
            counter += 1
        return counter, snap_invalidated

    @attr('manual-test', 'non-revert', 'story10831',
          'story10831_tc13', 'kgb-physical')
    def test_13_p_restore_snapshot_validity_chk_fail(self):
        """
        Description:
            To ensure that should a snapshot which was not
            created by LITP be corrupt on a node that the
            restore_snapshot task to check the validity of
            the snapshots passes.
        Actions:
            1. Ascertain which vxvm file systems should have snapshots
               created - that is those with snap_external=false,
               snap_size not = 0, and not of type swap.
            2. Create and deploy a new file-system
            3. Manually create a snapshot of the file system
               and invalidate it
            4. Issue the create_snapshot command.
            5. Verify that the snapshots have been created on the nodes,
               use vxprint -vt.
            6. Corrupt the snapshot of the new file-system.
            7. Issue the restore_snapshot command.
            8. Check the plan tasks
            9. Ensure the plan succeeds.
        """
        self.remove_all_snapshots(self.ms_node)
        file_sys_dict = self.retrieve_file_system_dict('vxvm')
        fs_url = ""
        active_node = ""
        vg_id = ""
        file_sys_urls = []
        cleanup_dict = {}
        self.log("info", "Starting action 1")
        # ASCERTAIN WHETHER AN VXFS WITH SNAP EXTERNAL FALSE EXISTS.
        file_sys_dict, cleanup_dict = \
        self.chk_and_update_snap_external(file_sys_dict)

        try:
            self.log("info", "Starting action 2")
            vg_id, mount_point, fs_url, already_exists = \
            self.create_new_file_system(file_sys_dict)
            if not already_exists:
                self.execute_cli_createplan_cmd(self.ms_node)
                self.execute_cli_runplan_cmd(self.ms_node)
                self.wait_for_plan_state(self.ms_node,
                                         test_constants.PLAN_COMPLETE)
            # UPDATE THE NEW FS TO BE SNAP_EXTERNAL TRUE
            self.execute_cli_update_cmd(self.ms_node, fs_url,
                                        props='snap_external=true')
            file_sys_dict = self.retrieve_file_system_dict('vxvm')

            self.log("info", "Starting action 3")
            self.manually_create_snap(vg_id, fs_url)

            self.log("info", "Starting action 4")
            self.execute_cli_createsnapshot_cmd(self.ms_node)
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                            test_constants.PLAN_COMPLETE))

            self.log("info", "Starting action 5")
            file_sys_dict = self.retrieve_file_system_dict('vxvm')
            nodes = file_sys_dict.keys()
            node = nodes[0]
            file_sys_urls = file_sys_dict[node]['false']['vxfs']
            for file_sys_url in file_sys_urls:
                self.verify_vxsnap_present(file_sys_url)

            self.log("info", "Starting action 6")
            active_node = self.get_active_node_for_vol_grp(vg_id)
            self.corrupt_snap_on_node(active_node, mount_point)
            counter = 0
            snap_invalidated = False
            # YOU MUST WAIT A FEW SECONDS FOR VCS TO IDENTIFY THE
            # INVALIDATED SNAP AND FLAG IT AS AN ERROR.
            while counter < 12 and snap_invalidated == False:
                counter, snap_invalidated = \
                self.chk_snap_validity(active_node, counter,
                                       snap_invalidated)
            self.assertTrue(snap_invalidated)

            self.log("info", "Starting action 7")
            try:
                self.execute_cli_restoresnapshot_cmd(self.ms_node)
                self.log("info", "Starting action 8")
                stdout, _, _ = \
                self.execute_cli_showplan_cmd(self.ms_node)
                self.log("info", "Starting action 9")
                self.chk_restore_plan_tasks(stdout, lvm_tasks=True)
            finally:
                self.execute_and_wait_restore_snapshot(self.ms_node,
                        skip_cmd=True)
        finally:
            # CLEANUP NEW FS SNAP EXTERNAL UPDATE
            self.execute_cli_update_cmd(self.ms_node, fs_url,
                                        props='snap_external',
                                        action_del=True)
            # CLEANUP ANY UPDATES
            if cleanup_dict != {}:
                self.cleanup_updates(cleanup_dict)
            self.execute_cli_removesnapshot_cmd(self.ms_node)
            # DELETE THE MANUALLY CREATED SNAPSHOT
            self.manually_remove_snap()
            # AFTER NODE BOOTS VCS TAKES A WHILE (~90 seconds)
            # TO IMPORT THE VOLUME GROUPS WHICH MANAGE THE
            # SNAPSHOTS. CREATE/REMOVE SNAPSHOT
            # IN CLEANUP WILL FAIL WITHOUT THIS WAIT.
            for file_sys_url in file_sys_urls:
                counter = 0
                node = None
                while counter < 20 and node is None:
                    vol_grp_url = \
                    self.get_vol_grp_from_vxfs_fs_url(file_sys_url)
                    vol_grp_id = \
                    self.get_vol_grp_id_from_url(vol_grp_url)
                    node = \
                    self.get_active_node_for_vol_grp(vol_grp_id,
                                                     assert_not_found=False)
                    counter += 1
                    time.sleep(10)

    def manually_create_snap(self, vg_id, fs_url):
        """
        Description:
            Issuses a series of vcs commands to create
            a snapshot and related objects for the
            specified volumge groups file system.
        Args:
            vg_id (str): the url id of the voume group
            fs_id (str): the url if of the file system
        """
        active_node = self.get_active_node_for_vol_grp(vg_id)
        fs_id = self.get_id_from_end_of_url(fs_url)
        self.vxassist_make_volume(active_node, vg_id, fs_id)
        self.vxmake_make_cache(active_node, vg_id, fs_id)
        self.vxcache_start_cache(active_node, vg_id, fs_id)
        self.vxsnap_make_snapshot(active_node, vg_id, fs_id)

    @staticmethod
    def get_vxassist_cmd(args=""):
        """
        Description:
            Creates the vxassist command.
        Args:
            args (str): the extra arguments to the cmd.
        Returns:
            str. the compiled cmd.
        """
        return "/usr/sbin/vxassist {0}".format(args)

    def vxassist_make_volume(self, node, vg_id, fs_id):
        """
        Description:
            Issues a vxassist cmd.
        Args:
            node (str): the node on which to exe the cmd.
            vg_id (str): the url id of the voume group
            fs_id (str): the url if of the file system
        """
        args = "-g {0} make LV{1}_ 14M init=active".format(vg_id, fs_id)
        cmd = self.get_vxassist_cmd(args)
        self.run_command(node, cmd,
                         su_root=True)

    def vxmake_make_cache(self, node, vg_id, fs_id):
        """
        Description:
            Issues a vxmake cmd.
        Args:
            node (str): the node on which to exe the cmd.
            vg_id (str): the url id of the voume group
            fs_id (str): the url if of the file system
        """
        args = \
        "-g {0} cache LO{1}_ cachevolname=LV{1}_".format(vg_id, fs_id)
        cmd = self.get_vxmake_cmd(args)
        self.run_command(node, cmd,
                         su_root=True)

    @staticmethod
    def get_vxmake_cmd(args=""):
        """
        Description:
            Creates the vxmake command.
        Args:
            args (str): the extra arguments to the cmd.
        Returns:
            str. the compiled cmd.
        """
        return "/usr/sbin/vxmake {0}".format(args)

    def vxcache_start_cache(self, node, vg_id, fs_id):
        """
        Description:
            Issues a vxcache cmd.
        Args:
            node (str): the node on which to exe the cmd.
            vg_id (str): the url id of the voume group
            fs_id (str): the url if of the file system
        """
        args = "-g {0} start LO{1}_".format(vg_id, fs_id)
        cmd = self.get_vxcache_cmd(args)
        self.run_command(node, cmd,
                         su_root=True)

    @staticmethod
    def get_vxcache_cmd(args=""):
        """
        Description:
            Creates the vxcache command.
        Args:
            args (str): the extra arguments to the cmd.
        Returns:
            str. the compiled cmd.
        """
        return "/sbin/vxcache {0}".format(args)

    @staticmethod
    def get_vxsnap_cmd(args=""):
        """
        Description:
            Creates the vxsnap command.
        Args:
            args (str): the extra arguments to the cmd.
        Returns:
            str. the compiled cmd.
        """
        return "/sbin/vxsnap {0}".format(args)

    def vxsnap_make_snapshot(self, node, vg_id, fs_id):
        """
        Description:
            Issues a vxsnap cmd.
        Args:
            node (str): the node on which to exe the cmd.
            vg_id (str): the url id of the voume group
            fs_id (str): the url if of the file system
        """
        args = \
        "-g {0} make source={1}/newvol=" \
        "L_{1}_/cache=LO{1}_".format(vg_id, fs_id)
        cmd = self.get_vxsnap_cmd(args)
        self.run_command(node, cmd,
                         su_root=True)

    def corrupt_snap_on_node(self, active_node, mount_point):
        """
        Description:
            Function to corrupt the file system snapshot
            on the specified node.
        Args:
            active_node (str): Node on which the cmd is
                               to be executed.
            mount_point (str): directory on the node
        """
        yes_cmd = \
        "/usr/bin/yes " \
        "abcdefghijklmnopqrstuvwxyz > " \
        "/{0}/test_file_10831".format(mount_point)
        self.run_command(active_node, yes_cmd,
                         su_root=True)
        self.del_file_after_run(active_node,
                                "/{0}/test_file_10831".format(mount_point))

    def manually_remove_snap(self):
        """
        Description:
            Function to manually remove snapshots
        """
        fss = self.get_all_volumes(self.ms_node)
        self.assertTrue(len(fss) > 0)

        active_node = self.get_vx_disk_node(self.ms_node,
                disk_group=fss[0]['volume_group_name'])
        url = fss[0]['path']
        snapshot_name = self.get_snapshot_name_from_url(url, 'vxvm')
        vol_grp_id = fss[0]['volume_group_name']
        cmd = \
        "/opt/VRTS/bin/vxedit -g {0} -rf rm {1}".format(vol_grp_id,
                snapshot_name)
        self.run_command(active_node, cmd, su_root=True, default_asserts=True)

    @staticmethod
    def get_vxprint_cmd(args=""):
        """
        Description:
            Compiles the vxprint cmd with the supplied args
        Args:
            args (str): arguments
        Returns:
            str. The compiled command
        """
        return "/sbin/vxprint {0}".format(args)

    @staticmethod
    def get_vol_grp_from_vxfs_fs_url(url):
        """
        Description:
            Function to return the url of the parent volume-group object of the
            supplied vxfs file-system object url.
        Args:
            url (str): LITP model url of a vxfs file-system object.
        Returns:
            str. A LITP model url of the volume-group object of the supplied
                 file-system url.
        """
        split_url = url.split('/')
        if 'infrastructure' in url:
            return "/".join([split_url[model_id] for model_id in range(0, 7)])
        else:
            return "/".join([split_url[model_id] for model_id in range(0, 9)])

    def get_vol_grp_id_from_url(self, url):
        """
        Description:
            Function to return the vol grp id from the file system
            url supplied.
        Args:
            url (str): The url to a vol group.
        Returns:
            split_url. str. The vol group id.
        """
        return self.get_id_from_end_of_url(url)

    def get_vol_grp_ids_from_urls(self, urls):
        """
        Description:
            Function to return the vol grp ids from the file system
            url supplied.
        Args:
            urls (list): The urls to  vol groups.
        Returns:
            list. The vol group ids.
        """
        vg_ids = []
        for url in urls:
            vg_ids.append(self.get_id_from_end_of_url(url))
        return vg_ids

    def get_vol_grps_from_vxfs_fs_urls(self, urls):
        """
        Description:
            Function to return the url of the parent volume-group object of the
            supplied vxfs file-system object url.
        Args:
            urls (list): A list of LITP model urls of a vxfs file-system object
        Returns:
            list. A list of LITP model urls of the volume-group objects of the
                  supplied file-system urls.
        """
        vg_urls = []
        for url in urls:
            vg_urls.append(self.get_vol_grp_from_vxfs_fs_url(url))
        return vg_urls

    @staticmethod
    def get_id_from_end_of_url(url):
        """
        Description:
            Function to return id from end of the url supplied.
        Args:
            url (str): The url to a file system.
        Returns:
            split_url. str. The id.
        """
        split_url = url.split('/')
        return split_url[-1]

    @staticmethod
    def get_vxdg_list(args=""):
        """
        Function to pass options to vxdg list command.
        Args:
            args (str): The optional argument to be passed.
        Returns:
            str. The command with or without the optional argument.
        """
        return "/sbin/vxdg {0} list".format(args)

    def get_active_node_for_vol_grp(self, vol_grp, assert_not_found=True):
        """
        Function to get the active node from the output of vxdg command.
        Args:
            vol_grp (str): The volume group that used is searched for active
                           node.
            assert_not_found (bool): Flag to state whether to assert.
        Returns:
            str. The name of the active node.
        """
        active_node_found = False
        for node in self.mn_nodes:
            # CHECK THAT NODE IS ONLINE BEFORE CONTINUING
            node_ip = self.get_node_att(node, 'ipv4')
            node_up = \
            self.wait_for_ping(node_ip, timeout_mins=1)
            if node_up:
                stdout, _, _ = \
                self.run_command(node,
                                 self.get_vxdg_list(), su_root=True)
                updated_stdout = []
                for item in stdout:
                    split_list_item = item.split(" ")
                    updated_stdout.extend(split_list_item)
                if vol_grp in updated_stdout:
                    return node
        if assert_not_found:
            self.assertTrue(active_node_found)

    def create_new_file_system(self, file_sys_dict):
        """
        Description:
            Function to create a new file system on a node;
            In order to pass all validation it is necessary
            for the file system to be inerited to a new
            vcs clustered service, thus this function
            shall create all of the required objects.
        Args:
            file_sys_dict (dict): A dictionary of all the
                                  vxvm file systems on the
                                  cluster.
        Returns:
            str. str. the volume group name, and the
                      mount point.
        """
        vg_name = "vg_10831"
        fs_name = "vxvm10831"
        new_fs_url = ""
        node = file_sys_dict.keys()[0]
        file_sys_urls = file_sys_dict[node]['false']['vxfs']
        vg_urls = \
        self.get_vol_grps_from_vxfs_fs_urls(file_sys_urls)
        # CHECK TO SEE IF THE VG HAS ALREADY been created
        for vg_url in vg_urls:
            if vg_name in vg_url:
                new_fs_url = vg_url + "/file_systems/{0}".format(fs_name)
                return vg_name, fs_name, new_fs_url, True
        vg_ids = self.get_vol_grp_ids_from_urls(vg_urls)
        self.log("info", "Getting free disk uuid's")
        uuid_dict = self.get_free_disk_uuids(vg_ids)
        sys_urls = []
        for node in self.mn_nodes:
            node_url = \
            self.get_node_url_from_filename(self.ms_node,
                                            node)
            sys_urls.append(
                self.deref_inherited_path(self.ms_node,
                                          node_url + "/system"))
        nodes = uuid_dict.keys()[0]
        disk = None
        uuid = None
        size = None
        if uuid_dict[nodes[0]] != {}:
            disk = uuid_dict[nodes[0]][0]
            uuid = uuid_dict[disk]
            self.log("info", "Using disk {0}".format(disk))
            size = self.get_disk_size(self.mn_nodes[0], disk)
        else:
            disk = uuid_dict[nodes[1]][0]
            uuid = uuid_dict[disk]
            self.log("info", "Using disk {0}".format(disk))
            size = self.get_disk_size(self.mn_nodes[1], disk)
        disk_name = "hd10831"
        props = \
        "name={0} bootable=false uuid={1} size={2}".format(disk_name,
                                                           uuid, size)
        for sys_url in sys_urls:
            self.execute_cli_create_cmd(self.ms_node, sys_url + \
                                        "/disks/test_10831",
                                        "disk", props,
                                        add_to_cleanup=False)
        # CREATE THE NEW VG
        self.log("info", "Creating the new volume group")
        storage_profile_url = self.get_storage_prof_from_vg_url(vg_urls[0])
        infra_storage_profile_url = \
        self.deref_inherited_path(self.ms_node,
                                  storage_profile_url)

        props = " volume_group_name={0}".format(vg_name)
        new_vg_url = \
        infra_storage_profile_url + "/volume_groups/{0}".format(vg_name)
        self.execute_cli_create_cmd(self.ms_node, new_vg_url,
                                    "volume-group", props,
                                    add_to_cleanup=False)
        new_fs_url = \
        new_vg_url + "/file_systems/{0}".format(fs_name)
        props = \
        "type=vxfs mount_point=/{0} size=20M " \
        "snap_size=70 snap_external=false".format(fs_name)
        self.execute_cli_create_cmd(self.ms_node, new_fs_url,
                                    "file-system", props,
                                    add_to_cleanup=False)
        new_pd_url = new_vg_url + "/physical_devices/10831"
        props = "device_name={0}".format(disk_name)
        self.execute_cli_create_cmd(self.ms_node,
                                    new_pd_url,
                                    "physical-device",
                                    props, add_to_cleanup=False)
        # CREATE THE VCS CLUSTERED SERVICE AND ALL THE RELATED OBJECTS
        self.log("info", "Creating the new VCS-CS")
        cs_name = "cs_10831"
        new_cs_url = \
        self.vcs_cluster_url + "/services/{0}".format(cs_name)
        node_list = \
        self.get_vcs_node_list_by_filename(self.mn_nodes)
        node_list_str = ",".join(node_list)
        props = \
        "active=1 standby=1 name={0} online_timeout=45 " \
        "node_list='{1}'".format(cs_name, node_list_str)
        self.execute_cli_create_cmd(self.ms_node, new_cs_url,
        "vcs-clustered-service", props, add_to_cleanup=False)
        filelist = []
        list_of_lsb_rpms = [
            "EXTR-lsbwrapper1-1.0.0.rpm"]
        for rpm in list_of_lsb_rpms:
            filelist.append(self.get_filelist_dict(
                                self.rpm_src_dir + rpm,
                                "/tmp/"))

        self.copy_filelist_to(self.ms_node, filelist,
                              add_to_cleanup=False,
                              root_copy=True)

        # Use LITP import to add to repo for each RPM
        for rpm in list_of_lsb_rpms:
            self.execute_cli_import_cmd(
                self.ms_node,
                '/tmp/' + rpm,
                self.repo_dir_3pp)
        rpm_name = "EXTR-lsbwrapper1"
        rpm_url = "/software/items/{0}".format(rpm_name)
        rpm_version = "1.0.0-1"
        props = \
        "name={0} version=\"{1}\"".format(rpm_name, rpm_version)
        self.execute_cli_create_cmd(self.ms_node, rpm_url,
                                    "package", props,
                                    add_to_cleanup=False)
        service_name = "test-lsb-01"
        service_url = "/software/services/{0}".format(service_name)
        props = \
        "cleanup_command='/bin/touch /tmp/{0}.cleanup' service_name='{0}' " \
        "stop_command='/sbin/service {0} stop' " \
        "status_command='/sbin/service " \
        "{0} status' start_command='/sbin/service {0} " \
        "start'".format(service_name)
        self.execute_cli_create_cmd(self.ms_node,
                                    service_url, "service", props,
                                    add_to_cleanup=False)
        self.execute_cli_inherit_cmd(self.ms_node, service_url + \
                                     "/packages/{0}".format(rpm_name),
                                     rpm_url, add_to_cleanup=False)
        self.execute_cli_inherit_cmd(self.ms_node, new_cs_url + \
                                     "/applications/{0}".format(service_name),
                                     service_url, add_to_cleanup=False)

        # INHERIT THE NEW F-S BELOW THE NEW VCS-CLUSTERED-SERVICE
        new_fs_cluster_url = \
        storage_profile_url + \
        "/volume_groups/{0}/file_systems/{1}".format(vg_name, fs_name)
        self.execute_cli_inherit_cmd(self.ms_node,
                                     new_cs_url + \
                                     "/filesystems/{0}".format(
                                                     fs_name),
                                     new_fs_cluster_url,
                                     add_to_cleanup=False)
        return vg_name, fs_name, new_fs_url, False

    def get_vcs_node_list_by_filename(self, nodes):
        """
        Description:
            Retrieves the node url id's which are used
            in the node list property of a vcs clustered service
        Args:
            nodes (str): The node filenames
        Returns:
            list.
        """
        node_ids = []
        for node in nodes:
            node_url = \
            self.get_node_url_from_filename(self.ms_node, node)
            node_ids.append(self.get_id_from_end_of_url(node_url))
        return node_ids

    @staticmethod
    def get_storage_prof_from_vg_url(vg_url):
        """
        Description:
            Function to return the storage profile url of the
            supplied volume group url supplied.
        Args:
            url (string): Url to the volume group
        Returns:
            str. url to the storage profileresides.
        """
        split_url = vg_url.split('/')
        tidied_url = [x for x in split_url if x != '']
        counter = 0
        new_url = ""
        if "/deployments" in vg_url:
            while counter < 6:
                new_url = new_url + "/{0}".format(tidied_url[counter])
                counter += 1
            return new_url
        else:
            while counter < 4:
                new_url = new_url + "/{0}".format(tidied_url[counter])
                counter += 1
            return new_url

    def get_disk_size(self, node, disk_name):
        """
        Description:
            Retrieves the size of the disk
            identified by the uuid specified.
        Args:
            node (str): Node on which to search.
            disk_name (str): disk from which size is sought.
        Returns:
            str. The size of the disk.
        """
        cmd = "/sbin/fdisk -l /dev/{0}".format(disk_name)
        stdout, _, _ = \
        self.run_command(node, cmd, su_root=True)
        size = stdout[0].split(" ")[2].split(".")[0]
        if "GB" in stdout[0]:
            return size + "G"
        else:
            return size + "M"

    @staticmethod
    def get_vxdisk_list(args=""):
        """
        Function to pass options to vxdisk list command.
        Args:
            args (str): The optional argument to be passed.
        Returns:
            str. The command with or without the optional argument.
        """
        return "/sbin/vxdisk {0} list".format(args)

    def get_free_disk_uuids(self, volg_grp_ids):
        """
        Description:
            Function to get the uuids for all of the shared disks in the
            cluster.
        Args:
            volg_grp_ids (list): A list of names of all of the volume group
                                 id's as they appear in the LITP model.
        Returns:
            dict. A dictionary with Key of the disk name and value of the uuid.
        """
        cmd = self.get_vxdisk_list("-e -o alldgs")
        nodes_disks = {}
        for node in self.mn_nodes:
            stdout, _, _ = \
            self.run_command(node, cmd, su_root=True)
            nodes_disks[node] = stdout
        self.log("info", "Compiling list of disks")
        disks_list = self.compile_all_nodes_disks(nodes_disks, volg_grp_ids)
        # ASSERT THAT NO UNASSIGNED SHARED DISKS EXISTS.
        self.assertNotEqual([], disks_list)
        shared_disks = set(disks_list[0] & disks_list[1])

        # ASSERT THAT AT LEAST ONE SHARED DISK HAS BEEN FOUND
        self.assertNotEqual(0, len(shared_disks))
        self.log("info",
                 "Retrieve the local disk names for the shared disks")
        # ["sdg"]
        # {"emc_clariion0_127": "sdg"}
        disk_names_node1 = \
        self.get_local_disks_from_vxvm_for_shared_disks(
                                            nodes_disks[self.mn_nodes[0]],
                                            shared_disks)
        # ["sdn"]
        # {"emc_clariion0_127": "sdn"}
        disk_names_node2 = \
        self.get_local_disks_from_vxvm_for_shared_disks(
                                            nodes_disks[self.mn_nodes[1]],
                                            shared_disks)

        self.log("info",
        "Local disk names - node 1: {0}".format(
                        ", ".join([disk_names_node1[disk] for disk
                                  in disk_names_node1])))

        self.log("info",
        "Local disk names - node 2: {0}".format(
                        ", ".join([disk_names_node2[disk] for disk
                                  in disk_names_node2])))

        cmd = "/bin/ls -la /dev/disk/by-id"
        stdout, _, _ = self.run_command(self.mn_nodes[0], cmd, su_root=True)

        # IF A BY-ID ENTRY IS NOT FOUND FOR A SHARED DISK ON THIS
        # NODE THEN THE OTHER NODE NEEDS TO BE CHECKED
        relevant_entries, relevant_dict, unfound_disks = \
        self.retrieve_relevant_disk_by_id_entries(disk_names_node1,
                                                  stdout)

        # IF ANY DISKS WERE NOT FOUND IN BY-ID THEN CHECK THIS
        # OTHER NODE.
        unfound_disk_names = {}
        if unfound_disks != []:
            for disk_name in unfound_disks:
                if disk_name in disk_names_node2.keys():
                    unfound_disk_names[disk_name] = \
                    disk_names_node2[disk_name]
                    continue
                else:
                    self.log("info",
                             "No entry has been found on" \
                             " either node for {0}".format(disk_name))
                    self.assertTrue(False)
            self.log("info",
                     "Checking other node for " \
                     "unfound disks {0}".format(", ".join(
                                     unfound_disk_names[disk_name]
                                     for disk_name in unfound_disk_names)))
            stdout, _, _ = \
            self.run_command(self.mn_nodes[1], cmd, su_root=True)
            relevant_entries_unfound, relevant_dict_unfound, unfound_disks = \
            self.retrieve_relevant_disk_by_id_entries(unfound_disk_names,
                                                      stdout)

            self.assertNotEqual([], relevant_entries_unfound)
            self.assertNotEqual({}, relevant_dict_unfound)
        self.assertEqual([], unfound_disks)
        scsi_entries = []
        scsi_dict = {}
        for entry in relevant_entries:
            split_list = entry.split(" ")
            tidied_list = [x for x in split_list if x != ""]
            scsi_entries.append(tidied_list[8])
            scsi_dict[relevant_dict[entry]] = tidied_list[8]
        self.assertNotEqual([], scsi_entries)
        self.log("info",
                 "Relevant entries: \n {0}".format(
                  "\n ".join([scsi_disk for scsi_disk
                              in scsi_entries])))
        self.assertNotEqual({}, scsi_dict)

        if relevant_entries_unfound != []:
            scsi_entries_for_unfound = []
            scsi_dict_for_unfound = {}
            for entry in relevant_entries_unfound:
                split_list = entry.split(" ")
                tidied_list = [x for x in split_list if x != ""]
                scsi_entries_for_unfound.append(tidied_list[8])
                scsi_dict_for_unfound[relevant_dict_unfound[entry]] = \
                tidied_list[8]
            self.assertNotEqual([], scsi_entries_for_unfound)
            self.log("info",
                     "Relevant entries on other node: \n {0}".format(
                      "\n ".join([scsi_disk for scsi_disk
                                  in scsi_entries_for_unfound])))
            self.assertNotEqual({}, scsi_dict_for_unfound)
        uuid_dict = {self.mn_nodes[0]: {}, self.mn_nodes[1]: {}}
        for disk in scsi_dict.keys():
            uuid_dict[self.mn_nodes[0]][disk] = \
            self.strip_uuid_from_scsi_entry(scsi_dict[disk])

        for disk in scsi_dict_for_unfound.keys():
            uuid_dict[self.mn_nodes[1]][disk] = \
            self.strip_uuid_from_scsi_entry(scsi_dict_for_unfound[disk])

        self.assertNotEqual({self.mn_nodes[0]: {}, self.mn_nodes[1]: {}},
                            uuid_dict)
        return uuid_dict

    @staticmethod
    def retrieve_relevant_disk_by_id_entries(disk_names_dict,
                                             by_id_stdout):
        """
        Description:
            Strips the relevant entries from the by-id entry
        Args:
            disk_names_dict (dict): key emc name, value linux name.
            by_id_stdout (list): the output of an ls la of by-id
        Returns:
            list. dict. list.
        """
        relevant_entries = []
        relevant_dict = {}
        unfound_disks = []
        for disk in disk_names_dict.keys():
            disk_found = False
            linux_name = disk_names_dict[disk]
            for line in by_id_stdout:
                if re.search(linux_name + "$",
                             line) and not re.search("wwn",
                                                     line):
                    relevant_entries.append(line)
                    relevant_dict[line] = linux_name
                    disk_found = True
                    break
            if disk_found == False:
                unfound_disks.append(disk)
        return relevant_entries, relevant_dict, unfound_disks

    @staticmethod
    def compile_all_nodes_disks(nodes_disks, vg_ids):
        """
        Description:
            Function to get all the disks on the nodes which have not
            been designated for use with LVM, or assigned to a vxvm volume
            group.
        Args:
            nodes_disks (list): List of all the shared disks.
            vg_ids (list): The ids of all the volume groups of type vxvm as
                           they appear in the LITP model.
        Returns:
            list. A list of disk names as they appear in the vxdisk console.
        """
        disks_list = []
        for node in nodes_disks.keys():
            disks = []
            node_disks = nodes_disks[node]
            for disk_entry in node_disks:
                if "DEVICE" in disk_entry or "LVM" in disk_entry:
                    continue
                if "sliced" in disk_entry:
                    continue
                # CHECK TO ENSURE THAT THE DISK ISN'T ALREADY ASSIGNED TO A
                # VOLUME GROUP.
                disk_assigned = [str for vg in vg_ids if vg in disk_entry]
                if disk_assigned != []:
                    continue
                disk = disk_entry.split(":")[0].split(' ')[0]
                if disk not in disks:
                    disks.append(disk)
            disks_list.append(set(disks))
        return disks_list

    @staticmethod
    def strip_uuid_from_scsi_entry(scsi_entry):
        """
        Description:
            Function to strip the uuid value used in LITP from the Linux
            disk representation.
        Args:
            scsi_entry (str): The Linux representation of the disk.
        Returns
            str. The uuid used by LITP during the disk object creation.
        """
        return scsi_entry.split("-")[1][1:]

    @staticmethod
    def get_local_disks_from_vxvm_for_shared_disks(vxdisk_console,
                                                   shared_disks):
        """
        Description:
            Function to strip the Linux disk names from the vxvm console.
        Args:
            vxdisk_console (list): The output of the vxdisk list console.
            shared_disks (list): The name of the shared disks as they appear
                                 in the vxdisk console output.
        Returns:
        dict. key emc name : val - The Linux names of the shared disks.
        """
        entries = [entry for entry in vxdisk_console
                   for disk in shared_disks if disk in entry]
        disk_names = {}
        for entry in entries:
            split_list = entry.split(" ")
            tidied_list = [x for x in split_list if x != ""]
            disk_names[tidied_list[0]] = tidied_list[6]
        return disk_names
