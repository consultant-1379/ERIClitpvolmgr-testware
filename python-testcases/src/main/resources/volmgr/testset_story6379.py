"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     Jan 2014
@author:    Padraic Doyle
@summary:   Integration test for story 2481 As a LITP User I want ability to
            specify a name tag to the create_snapshot command so that I can
            easily identify/audit snapshots later.
"""
from litp_generic_test import GenericTest, attr
from storage_utils import StorageUtils
import test_constants


class Story6379(GenericTest):
    """
    As a LITP User I want ability to specify a name tag to the create_snapshot
    command so that I can easily identify/audit snapshots later.
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story6379, self).setUp()
        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()

        self.timeout_mins = 10
        self.storage = StorageUtils()

    def tearDown(self):
        """Runs for every test"""
        super(Story6379, self).tearDown()

    @attr('all', 'revert', 'story6379', 'story6379_tc03')
    def test_03_n_error_if_snapshot_already_exists_on_a_node(self):
        """
        @tms_id: litpcds_6379_tc03
        @tms_requirements_id: LITPCDS-6379
        @tms_title: create snapshot error - snapshot file already existing
        @tms_description:
            This test will verify that an error is reported if a snapshot of a
            volume already exists on a node.
        @tms_test_steps:
            @step: Delete the deployment snapshot
            @result: deployment snapshot is deleted
            @step: Manually create a snapshot with the target name on a node.
            @result: Snapshot is created
            @step: Attempt to create a named snapshot
            @result: Snapshot plan fails
            @result: There is an error in the logs
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Delete the deployment snapshot')
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        node = self.mn_nodes[0]
        ss_name = "ss3"

        self.log('info',
            'Manually create a snapshot with the target name on a node.')
        fss = self.get_all_volumes(self.ms_node, vol_driver="lvm")
        vg_id = (x['vg_item_id'] for x in fss if
                x['node_name'] == node).next()
        ss_vol_name = "L_{0}_root_{1} ".format(vg_id, ss_name)
        args = ("-L 100M -s -n {0} ".format(ss_vol_name) +
                test_constants.LITP_SNAPSHOT_PATH +
                "{0}_root".format(vg_id))
        cmd = self.storage.get_lvcreate_cmd(args)

        out, err, ret_code = self.run_command(node, cmd, su_root=True)
        self.assertEqual(0, ret_code)
        self.assertEqual([], err)
        self.assertTrue(self.is_text_in_list('created', out))

        self.log('info', 'Attempt to create a named snapshot')
        args = "-n {0}".format(ss_name)

        self.execute_cli_createsnapshot_cmd(self.ms_node, args)

        log_msg = "already exists in volume group"

        # Verify that the message log contains a message indicating why
        # the snapshot failed to create.
        self.assertTrue(self.wait_for_log_msg(self.ms_node, log_msg))

        # Verify that the plan fails.
        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                                 test_constants.PLAN_FAILED,
                                                 self.timeout_mins)
                        )

    @attr('all', 'revert', 'story6379', 'story6379_tc04')
    def test_04_n_task_fails_if_no_enough_space_available(self):
        """
        @tms_id: litpcds_6379_tc04
        @tms_requirements_id: LITPCDS-6379
        @tms_title: create snapshot error - not enough space
        @tms_description:
            This test will verify that the task for snapshotting a volume fails
            if there is not enough space available for the snapshot.
        @tms_test_steps:
            @step:  Create snapshots until the volume group is full and
                the plan fails.
            @result: Eventually the plan fails
            @result: There is an error in the test
            @result: Snapshot item is in initial state
            @result: Timestamp property is empty
            @step: Run create_plan
            @result: DoNothingPlanError is returned
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        success = True
        index = 1
        log_path = test_constants.GEN_SYSTEM_LOG_PATH
        log_len = self.get_file_len(self.ms_node, log_path)

        self.log('info',
            'Create snapshots until Volume Group is full and plan fails.')
        while success:
            ss_name = "ss4_{0}".format(index)

            # Execute create_snapshot command
            args = "--name {0}".format(ss_name)
            self.execute_cli_createsnapshot_cmd(self.ms_node, args)
            index += 1
            try:
                # Verify that the create snapshot plan succeeds
                self.assertTrue(self.wait_for_plan_state(self.ms_node,
                        test_constants.PLAN_COMPLETE, self.timeout_mins))
            except AssertionError:
                success = False
                # Set expected message in message log
                log_msg = ('has insufficient free space')

                self.assertTrue(self.wait_for_log_msg(
                        self.ms_node, log_msg, timeout_sec=20,
                        log_len=log_len))

                self.assertEqual(self.get_item_state(self.ms_node,
                    "/snapshots/{0}".format(ss_name)), "Initial")
                self.assertEqual("",
                    self.get_props_from_url(self.ms_node,
                    "/snapshots/{0}".format(ss_name), "timestamp")
                    )
                self.log('info', 'Attempt to run create_plan.')
                _, err, _ = self.execute_cli_createplan_cmd(
                    self.ms_node, expect_positive=False
                )

                # Verify that Litp replies with DoNothingPlanError
                self.assertTrue(
                    self.is_text_in_list(
                        "DoNothingPlanError    "
                        "Create plan failed: no tasks were generated",
                        err
                    )
                )

    @attr('all', 'revert', 'story6379', 'story6379_tc06')
    def test_06_p_valid_lengths_for_the_snapshot_name_tag(self):
        """
        @tms_id: litpcds_6379_tc06
        @tms_requirements_id: LITPCDS-6379
        @tms_title: create snapshot - valid length
        @tms_description:
            This test verifies the maximum snapshot name length
        @tms_test_steps:
            @step: Create a named snapshot with the maximum supported name
                length
            @result: snapshot can be created
            @step: Create a named snapshot with the maximum supported name
            length + 1
            @result: Validation error is returned
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        self.log('info',
            'Create a named snapshot with the maximum supported length')
        # For space reasons delete existing Deployment Snapshot if it exists.
        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        # Get model volume group item IDs, volume item IDs.
        fsyss = self.get_all_volumes(self.ms_node, vol_driver='lvm')

        #calculate the size of the longest unmodelled volume
        #Note "/software" is never snapshot
        longest_unmodelled_volume = len("vg_root" + "lv_var_lib_puppetdb")

        # Get the longest volume name.
        max_len = 0
        for fsys in fsyss:
            vol_len = (len(fsys['volume_group_name']) +
                       len(fsys['vg_item_id']) +
                       len(fsys['volume_name']))
            if vol_len > max_len:
                max_vol_len = vol_len

        if max_vol_len < longest_unmodelled_volume:
            max_vol_len = longest_unmodelled_volume
        # Add 1 for for underscore separator and 1 for "/" character
        max_vol_len += 2
        self.log("info", "Max LVM volume name length is {0}"
                 .format(max_vol_len))

        # Verify that the user can create a named snapshot with
        #     nametag length = (122 - (2 + len(longest volume name + grp))
        max_tag_len = 122 - (2 + max_vol_len)
        ss_name = "n" * max_tag_len

        self.execute_and_wait_createsnapshot(self.ms_node,
            args="-n {0}".format(ss_name))

        # Delete the snapshot
        self.execute_and_wait_removesnapshot(self.ms_node,
            args="-n {0}".format(ss_name))

        self.log('info',
            'Create a named snapshot with the maximum supported length + 1')

        # Verify that they cannot create a named snapshot with
        #     nametag length > (122 - (2 + len(longest volume name + grp))
        ss_name = "n" * (max_tag_len + 1)
        args = "--name {0}".format(ss_name)
        out, err, ret_code = self.execute_cli_createsnapshot_cmd(
            self.ms_node,
            args,
            expect_positive=False)

        # Verify cmd fails for > max_tag_lens.
        self.assertEqual(1, ret_code)
        self.assertEqual([], out)

        # Verify the error message.
        # Updating message error according to LITPCDS-11732
        expect_err = ('ValidationError    ' +
                      'Create snapshot failed: Snapshot ' +
                      'name tag cannot exceed {0} '.format(max_tag_len) +
                      'characters which is the maximum available length ' +
                      'for an ext4 or xfs file system.')
        self.assertTrue(self.is_text_in_list(expect_err, err))
