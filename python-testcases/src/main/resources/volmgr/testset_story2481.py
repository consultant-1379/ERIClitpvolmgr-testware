"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     Aug 2014
@author:    Padraic Doyle
@summary:   Integration test for story 2481 As a LITP User I want to create and
            delete a VXVM snapshot when the specific commands are executed.
"""
from litp_generic_test import GenericTest, attr
from storage_utils import StorageUtils
import test_constants


class Story2481(GenericTest):
    """
    As a LITP User I want to create and delete a VXVM snapshot when the
    specific commands are executed.
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story2481, self).setUp()
        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()

        self.sto = StorageUtils()

    def tearDown(self):
        """Runs for every test"""
        super(Story2481, self).tearDown()

    def _replace_vxsnap_run_plan_wait_for_error(self, file_contents,
            run_plan_method, log_message):
        """
        Generic method that replaces the vsnap binary, runs a snapshot plan and
        then asserts that a log messages appears in the logs and the plan fails

        file_contents: new content for the vxsnap command
        run_plan_method: (snapshot) function to be executed
        log_message: message to be searched for in the logs
        """
        fail_node = self.get_vx_disk_node(self.ms_node)

        # 1. Backup vxsnap binary
        self.backup_file(fail_node, test_constants.VXSNAP_PATH,
                    backup_mode_cp=False)

        # 2. Create dummy vxsnap file
        create_success = self.create_file_on_node(fail_node,
                                                  test_constants.VXSNAP_PATH,
                                                  file_contents,
                                                  su_root=True)
        self.assertTrue(create_success, "File could not be created")

        # 3. Run create_snapshot/remove_snapshot
        run_plan_method(self.ms_node)

        # 4. Verify Error from running vxsnap list.
        self.assertTrue(self.wait_for_log_msg(self.ms_node,
            log_message))

        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                                 test_constants.PLAN_FAILED))

    @attr('all', 'revert', 'story2481', 'story2481_tc01', 'kgb-physical')
    def test_01_p_create_snapshot_vxvm(self):
        """
        @tms_id: litpcds_2481_tc01
        @tms_requirements_id: LITPCDS-2481
        @tms_title: Create VxVM snapshot
        @tms_description:
            When the user runs litp create_snapshot snapshot volumes are added
            on the vxvm node
        @tms_test_steps:
            @step: Run litp create_snapshot
            @result: Snapshot plan runs and finishes successfully
            @step: Run vxsnap list on the vxvm node
            @result: vxvm snapshot has been created
            @step: Run litp create_snapshot
            @result: error is reported
            @step: Run litp remove_snapshot
            @result: Snapshot plan runs and finishes successfully
            @step: Run vxsnap list on the vxvm node
            @result: vxvm snapshot has been removed
            @step: Run litp remove_snapshot
            @result: error is reported
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', '1. Run create_snapshot')
        self.execute_and_wait_createsnapshot(self.ms_node)
        fss = self.get_all_volumes(self.ms_node)
        disk_gs = set()

        for filesys in fss:
            vx_disk_node = self.get_vx_disk_node(self.ms_node,
                    disk_group=filesys["volume_group_name"])
            disk_gs.add(filesys["volume_group_name"])
            self.log('info',
            '2. Verify snapshot exists by executing vxsnap list on a node')
            cmd = self.sto.get_vxsnap_cmd(filesys['volume_group_name'],
                                              grep_args="L_")
            out, _, _ = self.run_command(vx_disk_node, cmd, su_root=True,
                    default_asserts=True)

            expect_ss_vol = "L_" + filesys["volume_name"] + "_"
            if filesys["snap_size"] != '0':
                self.assertTrue(self.is_text_in_list(expect_ss_vol, out),
                    '\nExpected volume "{0}" not found in\n{1}\n{2}'
                    .format(expect_ss_vol, cmd, '\n'.join(out)))
            else:
                self.assertFalse(self.is_text_in_list(expect_ss_vol, out),
                    '\nUnexpected volume "{0}" found in\n{1}\n{2}'
                    .format(expect_ss_vol, cmd, '\n'.join(out)))

        self.log('info', "# 3. Attempt to create another snapshot")
        _, stderr, _ = self.execute_cli_createsnapshot_cmd(
            self.ms_node,
            expect_positive=False
        )

        self.log('info', "# 4. Verify error is reported")
        self.assertTrue(
            self.is_text_in_list(
                "DoNothingPlanError    "
                "no tasks were generated. No snapshot tasks added",
                stderr
            )
        )

        self.log('info', "# 5. Run remove_snapshot.")
        self.execute_and_wait_removesnapshot(self.ms_node)

        self.log('info',
            "# 6. Verify snapshot removed by executing vxsnap list on a node")

        for disk_g in disk_gs:
            cmd = self.sto.get_vxsnap_cmd(disk_g, grep_args="L_")
            for node in self.mn_nodes:
                out, err, ret_code = self.run_command(node,
                                                      cmd,
                                                      su_root=True)
                self.assertEqual(1, ret_code)

                self.log('info',
                    "# 7. Verify no Error from running vxsnap list")

                self.assertEqual([], err)
                self.assertFalse(
                    self.is_text_in_list(
                        "L_", out)
                )

        self.log('info', "# 8. Attempt to remove_snapshot again.")
        _, err, _ = self.execute_cli_removesnapshot_cmd(
            self.ms_node, expect_positive=False)

        self.log('info', "# 9. Verify that an error is reported.")
        self.assertTrue(
            self.is_text_in_list(
                "DoNothingPlanError    no tasks were generated. No remove "\
                "snapshot tasks added because Deployment Snapshot does "\
                "not exist",
                err
            )
        )

    @attr('all', 'revert', 'story2481', 'story2481_tc05', 'kgb-physical')
    def test_05_p_vxsnap_no_return(self):
        """
        @tms_id: litpcds_2481_tc05
        @tms_requirements_id: LITPCDS-2481
        @tms_title: Error create VxVM snapshot - vxsnap not returning
        @tms_description:
            This test verifies that an error is returned by litp if the vxsnap
            command doesn't return for create_snapshot
        @tms_test_steps:
            @step: Replace vxsnap binary on peer nodes with dummy script that
                does not return
            @result: vxsnap binary is replaced
            @step: Run litp create_snapshot
            @result: snapshot plan fails
            @result: and error is written to the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        log_msg = "CallbackExecutionException running " \
                "task: Create VxVM deployment snapshot"
        file_contents = ['#!/usr/bin/env python',
                         'import time',
                         "for i in range(0, 700):",
                         "    time.sleep(1)"]

        self._replace_vxsnap_run_plan_wait_for_error(file_contents,
                self.execute_cli_createsnapshot_cmd, log_msg)

    @attr('all', 'revert', 'story2481', 'story2481_tc07', 'kgb-physical')
    def test_07_n_vxsnap_error(self):
        """
        @tms_id: litpcds_2481_tc07
        @tms_requirements_id: LITPCDS-2481
        @tms_title: Error create VxVM snapshot - vxsnap error
        @tms_description:
            This test verifies that an error is returned by litp if the vxsnap
            command returns an error for create_snapshot
        @tms_test_steps:
            @step: Replace vxsnap binary on peer nodes with dummy script that
                returns an error
            @result: vxsnap binary is replaced
            @step: Run litp create_snapshot
            @result: snapshot plan fails
            @result: and error is written to the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated

        Description:
            This test verifies that the task for snapshotting a volume fails
            if the vxsnap command returns an error.
        Actions:
            1. Remove existing snapshot
            2. Verify that the snapshot plan succeeds.
            3. Backup/move vxsnap
            4. Create dummy vxsnap file
            5. Run create_snapshot
            6. Verify that the snapshot plan fails.
            7 Verify that the message log contains an error
        """
        fail_node = self.get_vx_disk_node(self.ms_node)
        hostname = self.get_node_att(fail_node, "hostname")
        log_msg = \
           "Exception message: '{0} failed with message: vxsnap failed".format(
                hostname)

        file_contents = ['#!/bin/sh',
                        'if [[ $3 = "list" ]]',
                        'then',
                                '/sbin/vxsnap $@',
                        'else',
                                'echo "vxsnap failed 7" >&2',
                                'exit 7',
                        'fi']

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        self._replace_vxsnap_run_plan_wait_for_error(file_contents,
                self.execute_cli_createsnapshot_cmd, log_msg)

    @attr('all', 'revert', 'story2481', 'story2481_tc11', 'kgb-physical')
    def test_11_p_vxsnap_no_return(self):
        """
        @tms_id: litpcds_2481_tc11
        @tms_requirements_id: LITPCDS-2481
        @tms_title: Error remove VxVM snapshot - vxsnap not returning
        @tms_description:
            This test verifies that an error is returned by litp if the vxsnap
            command doesn't return for remove_snapshot
        @tms_test_steps:
            @step: run litp create_snapshot
            @result: snapshot is created
            @step: Replace vxsnap binary on peer nodes with dummy script that
                does not return
            @result: vxsnap binary is replaced
            @step: Run litp remove_snapshot
            @result: snapshot plan fails
            @result: and error is written to the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        if not self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_createsnapshot(self.ms_node)

        file_contents = ['#!/usr/bin/env python',
                         'import time',
                         "for i in range(0, 700):",
                         "    time.sleep(1)"]
        log_msg = \
                "CallbackExecutionException running task: " \
                "Remove VxVM deployment snapshot "

        self._replace_vxsnap_run_plan_wait_for_error(file_contents,
                self.execute_cli_removesnapshot_cmd, log_msg)
