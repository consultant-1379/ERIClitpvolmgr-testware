'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     March 2014
@author:    gabor
@summary:   Integration test for LVM volume group and filesystem creation
            Agile: LITPCDS-3153
'''


from litp_generic_test import GenericTest, attr


class Story3153(GenericTest):

    '''
    As a LITP Plugin developer, I want the LVM configuration created
    by Anaconda to be reflected in Puppet manifests, so I can later extend
    LVM functionality through puppet
    '''

    def setUp(self):
        """
        Description:
            Runs before every single test
        Actions:
            1. Call the super class setup method
            2. Set up variables used in the tests
        Results:
            The super class prints out diagnostics and variables
            common to all tests are available.
        """
        # 1. Call super class setup
        super(Story3153, self).setUp()
        self.test_node = self.get_management_node_filename()

    def tearDown(self):
        """
        Description:
            Runs after every single test
        Actions:
            1. Perform Test Cleanup
            2. Call superclass teardown
        Results:
            Items used in the test are cleaned up and the
        """
        super(Story3153, self).tearDown()

    def is_root_vg(self, vg_path):
        """
        Description:
            Decide volume group is root_vg
        Args:
            vg_path (str): volume group path
        Results:
            True if volume group is the root-vg, False otherwise
        """
        file_systems = self.find(self.test_node, vg_path, "file-system")
        for file_system in file_systems:
            # GET FILE SYSTEM DATA
            mount = self.get_props_from_url(self.test_node, file_system,
                                            "mount_point")
            if mount == "/":
                return True

        return False

    @attr('all', 'revert', 'story3153', '3153_02', 'cdb_priority1')
    def test_02_p_lvm_root_vg(self):
        """
        @tms_id: litpcds_3153_tc02
        @tms_requirements_id: LITPCDS-3153
        @tms_title: LVM root volume group
        @tms_description:
            This test ensures root volume group defined by the model
            has been created on each node
        @tms_test_steps:
            @step: Get all root volume groups in the model for each node
            @result: Volume groups exist in model
            @step: get vgdisplay output for every node
            @result: vgdisplay shows the modelled volume group
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # GET NODES
        nodes = self.find(self.test_node, "/deployments", "node")

        for node_path in nodes:

            # GET NODE STORAGE PROFILE NAME
            storage_path = node_path + "/storage_profile"
            storage_props = self.get_props_from_url(self.test_node,
                                                    storage_path)
            self.assertFalse(storage_props is None)
            infra_profile_path = self.deref_inherited_path(self.test_node,
                                                           storage_path)

            # GET VOLUME GROUPS
            vgs = self.find(self.test_node, infra_profile_path, "volume-group")
            for vg_path in vgs:

                # SKIP VGS OTHER THAN ROOT VG
                if not self.is_root_vg(vg_path):
                    continue

                # GET VG NAME
                vg_props = self.get_props_from_url(self.test_node, vg_path)
                self.assertFalse(vg_props is None)
                vg_name = vg_props["volume_group_name"]
                self.log("info", "ROOT VG: {0}".format(vg_name))

                # RUN PVDISPLAY ON EACH NODE
                test_node = self.get_node_filename_from_url(self.test_node,
                                                            node_path)
                self.assertFalse(test_node is None)
                cmd = "/sbin/vgdisplay | grep 'VG Name'"
                stdout, stderr, exit_code = \
                    self.run_command(test_node, cmd, su_root=True)
                self.assertEqual(0, exit_code)
                self.assertEqual([], stderr)

                # CHECK ROOT VG APPEARS IN VGDISPLAY OUTPUT
                self.assertTrue(self.is_text_in_list(vg_name, stdout))
