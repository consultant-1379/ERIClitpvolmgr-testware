'''
COPYRIGHT Ericsson 2023
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     June 2023
@author:    Ruth Evans
@summary:   TORF-639194 Verify all LVs in GRUB config
'''

from litp_generic_test import GenericTest, attr
from redhat_cmd_utils import RHCmdUtils
import test_constants


class Story639194(GenericTest):
    """
       As a Litp user I want to add all LVs to the
           kernel boot command in GRUB config
    """

    def setUp(self):
        """Setup variables for every test"""
        super(Story639194, self).setUp()

        self.rh_os = RHCmdUtils()
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()

        self.grub_default_file = "/etc/default/grub"
        self.grub_sfha_file = "/etc/grub.d/03_vxdmp_config_script"

        self.vmlinuz = " vmlinuz "
        self.cmdline = " GRUB_CMDLINE_LINUX "

    def tearDown(self):
        """Runs for every test"""
        super(Story639194, self).tearDown()

    def execute_grep(self, node, cmd, present=True):
        """
        Description:
            Function to execute grep command
        Args:
            node (str): node to run command on
            cmd (str): grep command
            present (Boolean): flag which indicates if grep
                               should be success or not
        Returns:
            None
        """
        stdout, stderr, rc = self.run_command(node, cmd, su_root=True)
        if present == True:
            self.assertEqual(0, rc)
            self.assertEqual([], stderr)
            self.assertNotEqual([], stdout)
        else:
            self.assertEqual(1, rc)
            self.assertEqual([], stderr)
            self.assertEqual([], stdout)

    def check_grub_files(self, fs_name, node, present=True):
        """
        Description:
            Function to check contents of grub files
        Args:
            fs_name (str)    : FS name to check for
            node (str)       : node to run command on
            present (Boolean): flag which indicates if FS
                               should be in the grub file or not
        Returns:
            None
        """

        # Check vmlinuz lines in grub.cfg
        cmd = self.rh_os.grep_path + self.vmlinuz + \
               test_constants.GRUB_CONFIG_FILE + "| " + \
               self.rh_os.grep_path + " " + fs_name
        self.execute_grep(node, cmd, present)

        # Check vmlinuz lines in sfha grub file
        cmd = self.rh_os.grep_path + self.vmlinuz + \
               self.grub_sfha_file + "| " + self.rh_os.grep_path + \
               " " + fs_name
        self.execute_grep(node, cmd, present)

        # Check cmd line default grub file
        cmd = self.rh_os.grep_path + self.cmdline + self.grub_default_file + \
               "| " + self.rh_os.grep_path + " " + fs_name
        self.execute_grep(node, cmd, present)

    @attr('all', 'revert', 'story639194', 'story639194_tc20', 'kgb-physical')
    def test_20_p_update_grub_lv_enable(self):
        '''
        @tms_id: TORF-639194_tc20
        @tms_requirements_id: TORF-639194
        @tms_title: Change grub_lv_enable to true
        @tms_description:
            Change grub_lv_enable to true and verify grub files
        @tms_test_steps:
            @step: Check value of grub_lv_enable is false
            @result: grub_lv_enable is false
            @step: Check contents of grub files
            @result: grub files do not contain non root and swap FS
            @step: Update grub_lv_enable to true, create and run plan
            @result: Plan runs successfully
            @step: Check plan contains task to update grub files
            @result: Plan contains expected tasks
            @step: Check contents of grub files
            @result: grub files contains all LVM FS from model
        @tms_test_precondition: N/A
        @tms_execution_type: Automated
        '''

        # Get list of FS names in model
        # Get a list of all FS
        # Get a list of all FS other than root and swap
        list_volumes = self.get_all_volumes(self.ms_node, vol_driver='lvm')
        self.assertTrue(len(list_volumes) > 0)
        fs_names = list()
        non_root_swap_fs_names = list()
        root_swap_name = ("root", "swap")
        for volume in list_volumes:
            fs_name = volume['volume_name']
            if fs_name not in root_swap_name:
                non_root_swap_fs_names.append(fs_name)
            if fs_name not in fs_names:
                fs_names.append(fs_name)

        vcs_cluster_url = self.find(self.ms_node,
                                         "/deployments", "vcs-cluster")[-1]

        # Check grub_lv_enable=false
        grub_lv_enable_value = self.execute_show_data_cmd(self.ms_node,
                                                       vcs_cluster_url,
                                                       "grub_lv_enable")
        self.assertEquals("false", grub_lv_enable_value)

        # Check contents of three grub files on MNs
        # files should not contain non root or swap FS
        for fs_name in non_root_swap_fs_names:
            for node in self.mn_nodes:
                self.check_grub_files(fs_name, node, False)

        self.log("info", "Update grub_lv_enable to true")
        self.execute_cli_update_cmd(self.ms_node,
                                    vcs_cluster_url,
                                    "grub_lv_enable=true")

        self.log("info", "Create and run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Check plan contains expected tasks")
        task_list = self.get_full_list_of_tasks(self.ms_node)
        task_descs = []
        for tasks in task_list:
            task_descs.append(tasks['MESSAGE'])

        for nodes in self.mn_nodes:
            expect_msg = 'Update LVM volume names in grub files on node "' \
                        + nodes + '" and re-build grub.'
            self.assertTrue(expect_msg in task_descs)

        # Check contents of three grub files on MNs
        # Each file should contain reference to each FS
        for fs_name in fs_names:
            for node in self.mn_nodes:
                self.check_grub_files(fs_name, node)
