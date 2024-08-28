"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     April 2014
@author:    Padraic Doyle
@summary:   Integration test for story 2478 As an administrator I want to
            remove a LVM snapshot that I no longer require.
            Agile: STORY-2478
"""
from litp_generic_test import GenericTest, attr
from redhat_cmd_utils import RHCmdUtils
from storage_utils import StorageUtils
import test_constants


class Story2478(GenericTest):
    """
    As an administrator I want to remove a LVM snapshot that I no longer
    require.
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story2478, self).setUp()
        # 2. Set up variables used in the test
        self.ms_nodes = self.get_management_node_filenames()
        self.ms_node = self.ms_nodes[0]
        self.mn_nodes = self.get_managed_node_filenames()
        self.all_nodes = self.ms_nodes + self.mn_nodes
        self.timeout_mins = 10
        self.rhcmd = RHCmdUtils()
        self.storage = StorageUtils()

    def tearDown(self):
        """Runs for every test"""
        super(Story2478, self).tearDown()

    def _get_snapshots(self, nodes):
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
        cmd = self.storage.get_lvscan_cmd(grep_args="L_")
        sshots = []
        for node in nodes:
            out, err, ret_code = self.run_command(node, cmd, su_root=True)
            self.assertEqual([], err)
            self.assertTrue(ret_code < 2)
            if out:
                vol_list = self.storage.parse_lvscan_stdout(out)
                for vol in vol_list:
                    sshots.append([node, vol['origin']])

        return sshots

    def _grub_bkup_exist(self, node):
        """
        Description:
            Check if grub backup exists.
        Actions:
                1. Verify that a grub.conf is backed_up.
        Results:
            Boolean, True if exists or False otherwise
        """
        grub_bkup = test_constants.GRUB_CONFIG_FILE + ".backup"
        # Returns a boolean, True if exists or False otherwise
        return self.remote_path_exists(node, grub_bkup, su_root=True)

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

    def _snapshot_all_nodes(self):
        '''
        Description:
            Remove any partial snapshot if it exists and create a new
            snapshot.
        '''
        if self._snapshot_item_exists():
            # 1. Run the litp remove_snapshot command
            self.execute_cli_removesnapshot_cmd(self.ms_node)

            # 2. Verify that the delete snapshot plan succeeds.
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                test_constants.PLAN_COMPLETE, self.timeout_mins))

        # 3. Create a new snapshot
        self.execute_cli_createsnapshot_cmd(self.ms_node)

        # 4. Verify that the create snapshot plan succeeds.
        self.assertTrue(self.wait_for_plan_state(self.ms_node,
            test_constants.PLAN_COMPLETE, self.timeout_mins))

    def _snapshot_item_exists(self):
        """
        Description:
            Determine if a snapshot item exists in the model.
        Results:
            Boolean, True if exists or False otherwise
         """
        snapshot_urls = self.find(self.ms_node, "/snapshots",
                              "snapshot-base", assert_not_empty=False)
        if snapshot_urls:
            return True
        else:
            return False

    @attr('all', 'revert', 'story2478', '2478_01')
    def test_01_p_remove_snapshot_removes_lvm_snapshots(self):
        """
        @tms_id: litpcds_2478_tc01
        @tms_requirements_id: LITPCDS-2478, LITPCDS-10830
        @tms_title: remove snapshot
        @tms_description:
            Verifies remove_snapshot removes snapshots.
        @tms_test_steps:
            @step: Add a package on a node
            @result: package exists in model
            @step: Run create_plan
            @result: Plan has been created
            @step: Run remove_snapshot
            @result: Snapshot plan is running
            @step: Run remove_plan
            @result: InvalidRequestError is returned
            @step: Get plan output
            @result: Remove_snapshot verification tasks are present
            @step: Wait for remove_snaphot plan to finish
            @result: Remove_snapshot plan succeeds
            @result: Snapshots are removed
            @result: snapshot item is removed
            @result: grub backup is removed
            @result: package is not added
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # Create a snapshot of all nodes.
        self._snapshot_all_nodes()

        # Verify that the grub backup exists.
        for node in self.all_nodes:
            self.assertTrue(self._grub_bkup_exist(node))

        self.log('info', 'Add a package on a node')
        # Create a package (telnet)
        package_url = self._create_package("telnet", True)

        # Select node to install package on
        node = self.mn_nodes[-1]

        # Get the URL of the node by matching hostname
        hostname = self.get_node_att(node, "hostname")
        nodes_urls = self.find(self.ms_node, "/deployments", "node")
        for node_url in nodes_urls:
            node_hostname = self.execute_show_data_cmd(self.ms_node,
                                                       node_url,
                                                       "hostname")
            if node_hostname == hostname:
                self.log("info", "Node url is {0}".format(node_url))
                found_node_url = node_url
                break

        # Link the package to a node.
        self._create_package_inheritance(found_node_url, "telnet", package_url)

        self.log('info', 'Run create_plan')
        self.execute_cli_createplan_cmd(self.ms_node)

        self.log('info', 'Run remove_snapshot')
        self.execute_cli_removesnapshot_cmd(self.ms_node)

        self.log('info', 'Run remove_plan')
        _, stderr, _ = self.execute_cli_removeplan_cmd(
            self.ms_node,
            expect_positive=False
        )

        # Verify that MS replies with InvalidRequestError
        self.assertTrue(
            self.is_text_in_list(
                "InvalidRequestError    "
                "Removing a running/stopping plan is not allowed",
                stderr
            ),
            "'InvalidRequestError' relating to removing "
            "plan when running plan"
        )

        #verify remove_snapshot verification tasks are present
        # LITPCDS-10830 addition
        stdout, _, _ = self.execute_cli_showplan_cmd(self.ms_node)
        self.assertFalse(
            self.is_text_in_list(
                "Check LVM snapshots on node(s)",
                 stdout))
        self.assertTrue(
            self.is_text_in_list(
                "Check peer nodes",
                stdout))
        self.assertTrue(
            self.is_text_in_list(
                "are reachable",
                stdout))

        self.log('info', "Wait for remove_snapshot plan to finish")
        self.assertTrue(self.wait_for_plan_state(self.ms_node,
            test_constants.PLAN_COMPLETE, self.timeout_mins))

        # Verify that the snapshots are removed
        sshot_list = self._get_snapshots(self.all_nodes)
        self.assertEqual([], sshot_list)

        # Verify that the snapshot item is removed from /snapshots
        self.assertFalse(self._snapshot_item_exists())

        # Verify that the grub backup is gone.
        for node in self.all_nodes:
            self.assertFalse(self._grub_bkup_exist(node))

        # Verify that the package is not added.
        chk_pkg_cmd = self.rhcmd.check_pkg_installed(['telnet'])
        out, err, ret_code = self.run_command(self.ms_node, chk_pkg_cmd,
                                              su_root=True)
        self.assertTrue(1, ret_code)
        self.assertEqual([], err)
        self.assertEqual([], out)

    @attr('all', 'non-revert', 'story2478', '2478_09')
    def test_09_n_error_if_lvremove_doesnt_return(self):
        """
        @tms_id: litpcds_2478_tc09
        @tms_requirements_id: LITPCDS-2478
        @tms_title: error if lvremove does not return
        @tms_description:
            This test will verify that an error is returned by litp
            if the lvremove command doesn't return.
        @tms_test_steps:
            @step: Replace lvremove with one that does not return
                  within the task timeout
            @result: lvremove is replaced
            @step: Run remove_snapshot
            @result: Remove_snapshot plan is running
            @step: Run remove_snapshot again
            @result: Invalid request error is returned
            @step: Wait for remove_snapshot plan to succeed
            @result: Snapshot plan fails
            @result: There is a log message
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # Store the current message log length.
        log_path = test_constants.GEN_SYSTEM_LOG_PATH
        log_len = self.get_file_len(self.ms_node, log_path)
        node = self.mn_nodes[-1]

        # Create a snapshot of all nodes
        self._snapshot_all_nodes()

        try:
            self.log('info', 'Replace lvremove with one that does not return '
                    'within the task timeout')
            # Backup/mv lvremove
            lvremove_path = self.storage.lvremove_path
            cmd = self.rhcmd.get_move_cmd(lvremove_path,
                                          (lvremove_path + "_old"))
            out, err, ret_code = self.run_command(node, cmd, su_root=True)
            self.assertEqual(0, ret_code)
            self.assertEqual([], err)
            self.assertEqual([], out)

            # Prepare empty lvremove file
            file_contents = ["#!/bin/sh",
                             "sleep 400"]
            create_success = self.create_file_on_node(node,
                self.storage.lvremove_path,
                file_contents,
                su_root=True,
                add_to_cleanup=False
            )
            self.assertTrue(create_success, "File could not be created")

            self.log('info', 'Run remove_snapshot')
            self.execute_cli_removesnapshot_cmd(self.ms_node)

            self.log('info', 'Run remove_snapshot again')
            _, err, _ = self.execute_cli_removesnapshot_cmd(self.ms_node,
                                                expect_positive=False)

            # Verify that the MS replies with InvalidRequestError
            self.assertTrue(self.is_text_in_list("InvalidRequestError    " +
                    "Plan already running", err))

            self.log('info', 'Wait for remove_snapshot plan to succeed')
            completed_successfully = self.wait_for_plan_state(
                self.ms_node,
                test_constants.PLAN_COMPLETE,
                self.timeout_mins
            )
            self.assertFalse(completed_successfully)

            # Verify that the message log contains a message indicating why
            #     the snapshot failed to delete.
            log_msg = "execution expired"
            curr_log_pos = self.get_file_len(self.ms_node, log_path)
            test_logs_len = curr_log_pos - log_len

            # Run grep on the server logs related to this test
            cmd = self.rhcmd.get_grep_file_cmd(log_path, log_msg,
                                               file_access_cmd="tail -n {0}"
                                               .format(test_logs_len))
            out, err, ret_code = self.run_command(self.ms_node,
                                                  cmd,
                                                  su_root=True)

            self.assertEqual(0, ret_code)
            self.assertEqual([], err)
            self.assertTrue(self.is_text_in_list(log_msg, out))
        finally:
            # Move lvremove to the proper location
            cmd = self.rhcmd.get_move_cmd((lvremove_path + "_old"),
                                          lvremove_path, True)

            self.run_command(node, cmd, su_root=True)

            # Delete the failed snapshot
            self.execute_cli_removesnapshot_cmd(self.ms_node)

            # Verify that the snapshot plan succeeds.
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                test_constants.PLAN_COMPLETE, self.timeout_mins))

    @attr('all', 'revert', 'story2478', '2478_11')
    def test_11_p_plan_success_stop_plan_while_remove_snaphot_running(self):
        """
        @tms_id: litpcds_2478_tc11
        @tms_requirements_id: LITPCDS-2478
        @tms_title: remove snapshot stop plan
        @tms_description:
            This test will verify that the plan stops if
            stop_plan is issued while remove_snaphot is running.
        @tms_test_steps:
            @step: Run remove_snapshot
            @result: Remove_snapshot plan is running
            @step: Run stop_plan
            @result: Plan is stopped
            @step: Run remove_snapshot
            @result: Plan finishes successfully
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # Create a snapshot of all nodes
        self._snapshot_all_nodes()

        self.log('info', 'Run remove_snapshot')
        self.execute_cli_removesnapshot_cmd(self.ms_node)

        self.log('info', 'Run stop_plan')
        self.execute_cli_stopplan_cmd(self.ms_node)

        # Verify that the plan stops successfully
        stopped_successfully = self.wait_for_plan_state(
            self.ms_node,
            test_constants.PLAN_STOPPED,
            self.timeout_mins
        )
        self.assertTrue(stopped_successfully)

        self.log('info', 'Run remove_snapshot')
        self.execute_cli_removesnapshot_cmd(self.ms_node)

        # Verify that the plan is successful
        completed_successfully = self.wait_for_plan_state(
            self.ms_node,
            test_constants.PLAN_COMPLETE,
            self.timeout_mins
        )
        self.assertTrue(completed_successfully)
