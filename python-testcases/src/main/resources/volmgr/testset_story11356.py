"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     March 2016
@author:    Jose Martinez, Jenny Schulze
@summary:   Integration test for story 11356.
            As a LITP user I want to extend my disk and the VxVM volumes that
            are on it so that I can increase capacity

            Agile: STORY-11356
"""
from litp_generic_test import GenericTest, attr
import test_constants


class Story11356(GenericTest):
    """
    As a LITP user I want to extend my disk and the VxVM volumes that are on it
    so that I can increase capacity.
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story11356, self).setUp()
        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()
        self.all_nodes = [self.ms_node] + self.mn_nodes

    def tearDown(self):
        """Runs for every test"""

        super(Story11356, self).tearDown()

    def _find_physical_disks(self):
        """
        finds a physical vxvm disk in the model
        """
        storage_profile = self.get_storage_profile_paths(self.ms_node,
                profile_driver='vxvm', path_url='/deployments')[-1]
        physical_devices = self.find(self.ms_node, storage_profile,
                'physical-device')
        num_disks = len(physical_devices)
        physical_device_name = self.get_props_from_url(self.ms_node,
                physical_devices[-1], filter_prop='device_name')

        actual_disks = self.find(self.ms_node, '/infrastructure/systems',
                'disk')
        disks_to_update = [d for d in actual_disks if
            (physical_device_name in
                self.get_props_from_url(self.ms_node, d, 'name'))]

        return num_disks, disks_to_update

    def _replace_vxdisk_increase_size(self, vxdisk_script, expected_error):
        """
        Description:
            This test verifies that when vxdisk command returns with error,
            the plan fails and there is a log message.
        Actions:
            1. Update disks sizes
            2. Replace vxdisk on all nodes with dummy vxdisk provided
            3. Create plan
            4. Run plan
            5. Wait for specified error message in log
        Result: The plan fails and there is a log message.

        :vxdisk_script: replacement script for vxdisk
        :expected error: error message that should appear in /var/log/messages
        """
        vxdisk_path = "/sbin/vxdisk"

        self.log('info', "1. Update disks sizes")
        _, disks_to_update = self._find_physical_disks()

        current_size = self.get_props_from_url(self.ms_node,
                   disks_to_update[-1], filter_prop='size')

        unit = current_size[-1]
        number = int(current_size[:-1])

        for disk in disks_to_update:
            self.backup_path_props(self.ms_node, disk)
            self.execute_cli_update_cmd(self.ms_node, disk,
                    props='size={0}{1}'.format(number + 1, unit))

        self.log('info', "2. Replace vxdisk on all nodes with dummy vxdisk")

        for node in self.mn_nodes:
            self.backup_file(node, vxdisk_path, backup_mode_cp=False)
            self.create_file_on_node(node, vxdisk_path, vxdisk_script,
                    su_root=True, add_to_cleanup=False)

        self.log('info', "3. Create plan")
        self.execute_cli_createplan_cmd(self.ms_node)

        self.log('info', "4. Run plan")
        self.execute_cli_runplan_cmd(self.ms_node)

        self.log('info',
        "5. Wait for error message {0} in log".format(expected_error))
        self.assertTrue(self.wait_for_log_msg(self.ms_node, expected_error))
        self.wait_for_plan_state(self.ms_node, test_constants.PLAN_FAILED)

    @attr('all', 'revert', 'story11356', 'story11356_tc16', 'kgb-physical')
    def test_16_n_vxdisk_error(self):
        """
        @tms_id: litpcds_11356_tc16
        @tms_requirements_id: LITPCDS-11356
        @tms_title: Extend vxvm disk - vxdisk error
        @tms_description:
            This test verifies that when the vxdisk command returns with error,
            the plan fails and there is a log message.
        @tms_test_steps:
            @step: Update the vxdisk sizes in the model
            @result: vxdisk sizes are updated
            @step: replace vxdisk binary on nodes with dummy vxdisk returning
                   an error
            @result: vxdisk binary is replaced on nodes
            @step: create/run plan
            @result: The plan fails
            @result: an error is logged
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        file_contents = [
                    '#!/bin/bash',
                    'if [[ "$@" == *resize* ]]',
                    'then',
                    '    exit 1',
                    'else',
                    '    exec /tmp/vxdisk $@',
                    'fi'
                   ]

        error_message = "failed without any error message but with status 1"

        self._replace_vxdisk_increase_size(file_contents, error_message)

    @attr('all', 'revert', 'story11356', 'story11356_tc17', 'kgb-physical')
    def test_17_n_vxdisk_timeout(self):
        """
        @tms_id: litpcds_11356_tc17
        @tms_requirements_id: LITPCDS-11356
        @tms_title: Extend vxvm disk - vxdisk timeout
        @tms_description:
            This test verifies that when the vxdisk command does not return,
            the plan fails and there is a log message.
        @tms_test_steps:
            @step: Update the vxdisk sizes in the model
            @result: vxdisk sizes are updated
            @step: replace the vxdisk binary on nodes with dummy vxdisk that
                   does not return within the timeout
            @result: vxdisk binary is replaced on nodes
            @step: create/run plan
            @result: The plan fails
            @result: an error is logged
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        file_contents = [
                    '#!/bin/bash',
                    'if [[ "$@" == *resize* ]]',
                    'then',
                    '    sleep 360',
                    'else',
                    '    exec /tmp/vxdisk $@',
                    'fi'
                   ]

        error_message = "No answer from node"

        self._replace_vxdisk_increase_size(file_contents, error_message)
