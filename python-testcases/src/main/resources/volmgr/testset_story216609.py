'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     Sep 2017
@author:    Gary O'Connor
@summary:   Integration
            Agile: STORY-216609
'''

from litp_generic_test import GenericTest, attr
import test_constants
from storage_utils import StorageUtils
from redhat_cmd_utils import RHCmdUtils


class Story216609(GenericTest):
    """
    As a LITP user, I want the ability specify nested paths for mount_point
    in the model, so that I have a convenient way of attaching LVM file
    systems
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
        super(Story216609, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()
        self.node_urls = self.find(self.ms_node, "/deployments", "node")
        self.node_urls.sort()
        self.storage = StorageUtils()
        self.rhcmd = RHCmdUtils()

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
        super(Story216609, self).tearDown()

    def get_vol_grp(self, url):
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
        self.assertNotEqual([], stdout)
        return stdout

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
        return url.split('/')[-1]

    def get_volume_name(self, fs_url):
        """
        Description:
            Function to compile and return the LVM
            volume name of the supplied file system.
        Args:
            fs_url (str): url to a file system on a node.
        Returns:
            str.
        """
        fs_id = self.get_id_from_end_of_url(fs_url)
        vg_url = self.find_parent_path_from_item_type(self.ms_node,
                                                      'volume-group',
                                                      fs_url)
        vg_id = self.get_id_from_end_of_url(vg_url)
        return "{0}_{1}".format(vg_id, fs_id)

    def verify_fs_created(self, node, fs_url, size="",
               mount_point="", negative_chk=False, is_ms=False):
        """
        Description:
            Function to verify that the file system has been created,
            the mount point created when applicable, the file
            system mounted when a mount point was created, and that
            the volume has been created.
        Args:
            node (str): filename of the node on which the cmds
                        are to be executed.
            fs_url (str): url to the file system.
            size (str): Size of the file system.
            mount_point (str): mount point to be created.
            negative_chk (bool): flag to whether to check for
                                 mount point creation.
        """
        lvs_cmd = self.storage.get_lvs_cmd()
        stdout, _, _ =\
        self.run_command(node, lvs_cmd, su_root=True)
        if is_ms:
            vg_name = "vg_root"
            fs_name = fs_url.split('/')[-1]
            volume_name = "".join([vg_name, '_', fs_name])

        else:
            volume_name = self.get_volume_name(fs_url)
            vg_url = self.find_parent_path_from_item_type(self.ms_node,
                                                      'volume-group',
                                                      fs_url)
            vg_name = self.get_props_from_url(self.ms_node, vg_url,
                                "volume_group_name")

        # ENSURE THE VOLUME HAS BEEN CREATED
        vol_found = False
        vol_size_correct = False
        for line in stdout:
            split_lines = line.split(" ")
            tidied_list = [x for x in split_lines if x != '']
            if volume_name in tidied_list:
                vol_found = True
                # CHECK THE SIZE OF THE VOLUME
                node_vol_size = tidied_list[3]
                formatted_size = \
                size[:-1] + ".00" + size[-1:].lower()
                if formatted_size == node_vol_size:
                    vol_size_correct = True
                break
        self.assertTrue(vol_found)
        self.assertTrue(vol_size_correct)

        # ENSURE THE MOUNT POINT HAS BEEN CREATED AND
        # ENSURE THE FILE SYSTEM HAS BEEN CREATED
        if mount_point != "":
            dir_list = \
            self.list_dir_contents(node, '/', su_root=True)
            found = \
            self.is_filesystem_mounted(node, mount_point)
            fs_listing = \
            "/dev/mapper/{0}-{1}".format(vg_name, volume_name)
            df_cmd = self.rhcmd.get_df_cmd("-Th")
            stdout, _, _ =\
            self.run_command(node, df_cmd, su_root=True)
            fs_found = False
            for line in stdout:
                split_lines = line.split(" ")
                tidied_list = [x for x in split_lines if x != '']
                if fs_listing in tidied_list:
                    fs_found = True
                    break
            if not negative_chk:
                self.assertTrue(self.is_text_in_list(mount_point[1:],
                                                     dir_list))
                self.assertTrue(found)
                self.assertTrue(fs_found)

    def get_node_file_from_fs_url(self, fs_url):
        """
        Description:
            Function to retrieve the node file name from
            the file system url supplied
        Args:
            fs_url(str): url to a file system on a node.
        Returns:
            str.
        """
        node_url = self.find_parent_path_from_item_type(self.ms_node, 'node',
                                                        fs_url)
        return self.get_node_filename_from_url(self.ms_node, node_url)

    def verify_properties(self, node_fs_urls):
        """
        Verifies mount_points and size properties
        """
        for fs_url in node_fs_urls:
            self.get_node_file_from_fs_url(fs_url)
            self.get_props_from_url(self.ms_node, fs_url)

    def create_new_fs(self):
        """
        Description:
            Function to perform creation of lvm file systems
            1) Below and existing storage-profile of type lvm that is used on
               all of the nodes to create 2 new file-systems fs_12270_1 and
               fs_12270_2 with nested mount_points specified.
            2)  Create & run plan.
            3)  Verify.

        Args:
            vg_id: volume group
            node_fs_urls(list): urls to lvm fs on nodes.
            file_system_info(dict): fs names and paths to fs
        """
        self.remove_all_snapshots(self.ms_node)
        derefd_urls = []
        # ENSURE THAT ALL THE NODES ARE USING THE SAME PROFILE
        for node in self.node_urls:
            derefd_url = \
            self.deref_inherited_path(self.ms_node, node + "/storage_profile")
            if derefd_urls != [] and derefd_url not in derefd_urls:
                # NODES ARE USING DIFFERENT STORAGE PROFILES
                self.assertTrue(False)
            else:
                derefd_urls.append(derefd_url)
        derefd_url = derefd_urls[0]
        vol_grps = \
        self.get_vol_grp(derefd_url)
        vg_id = self.get_id_from_end_of_url(vol_grps[0])

        # CREATE THE NEW FILE SYSTEM BELOW THIS PROFILE
        self.log("info",
            "Create 2 file systems with nested mount paths")

        fss = {"nested_fs1": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270/fs1",
                    },
               "nested_fs2": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270/fs2"
                    }
               }

        for fsystem, prop_dict in fss.iteritems():
            fs_url = "{0}/file_systems/{1}".format(vol_grps[0],
                                                fsystem)
            props = " ".join("{0}={1}".format(key, value) for (key, value) in
              prop_dict.iteritems())
            fss[fsystem]['url'] = fs_url
            self.execute_cli_create_cmd(self.ms_node,
                                    fs_url,
                                    "file-system",
                                    props)

        self.log("info", "Run the plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Ensure file systems have been created")
        node_fs_urls = []

        for node_url in self.node_urls:
            for fsystem in fss:
                node_fs_urls.append("{0}/storage_profile/"
                "volume_groups/{1}/file_systems/{2}".format(node_url,
                                    vg_id, fsystem))
        self.verify_properties(node_fs_urls)
        return vg_id, node_fs_urls, fss

    @attr('all', 'revert', 'story216609', 'story216609_tc01')
    def test_01_p_create_new_lvm_fs_with_nested_mnt_path(self):
        '''
        @tms_id: TORF-216609_tc01
        @tms_requirements_id: TORF-216609
        @tms_title: Create a lvm fs with a nested mount_path
        @tms_description:
            Test to create 2 new lvm FS with nested mount paths,
            once created and applied remove one of the fs with nested mount
            paths.
        @tms_test_steps:
            @step: Create 2 new lvm fs with nested mount paths
            @result: file systems are created in the model
            @step: Create/Run plan
            @result: Plan is created and runs to completion
            @result: mount_point is updated

            @step: Create and run the plan
            @result: Plan should run to completion
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        vg_id, node_fs_urls, _ = self.create_new_fs()
        vg_url = self.find_parent_path_from_item_type(self.ms_node,
            'volume-group', node_fs_urls[0])
        self.get_props_from_url(self.ms_node, vg_url,
                                "volume_group_name")

        self.log("info", "Verify mount point is recreated")
        # VERIFY NEW MOUNTS CREATED
        for node_url in self.node_urls:
            "{0}/storage_profile/volume_groups/{1}/file_systems/{2}"\
                .format(node_url, vg_id, 'nested_fs1')
            "{0}/storage_profile/volume_groups/{1}/file_systems/{2}" \
                .format(node_url, vg_id, 'nested_fs2')
            self.get_node_filename_from_url(self.ms_node, node_url)
        self.log("info", "New fs with nested mount paths created")
        self.run_command(self.ms_node,
                         "df -h",
                         )

    @attr('all', 'revert', 'story216609', 'story216609_tc02')
    def test_02_p_update_lvm_fs_with_new_nested_mnt_path(self):
        '''
        @tms_id: TORF-216609_tc02
        @tms_requirements_id: TORF-216609
        @tms_title: Update an existing lvm fs with a nested mount path to a
        new mount path
        @tms_description:
            Test to update an existing lvm FS's nested mount path.
        @tms_test_steps:
            @step: Update one of the lvm fs mount paths to a new mount path.
            @result: file systems are updated in the model with new mount path
            @step: Create and run plan
            @result: Plan is created and runs to completion
        @tms_test_precondition: Have 2 applied lvm fs in the model with nested
        mount paths.
        @tms_execution_type: Automated
        '''
        # Call create lvm fs method
        _, node_fs_urls, fs_info = self.create_new_fs()

        self.log("info",
            "Update a file system's mount_point under /infrastructure")
        # Update the existing lvm fs with a new nested mount point
        file_system_name_1_updated = "/fs_12270/fs1_updated/fs"
        self.execute_cli_update_cmd(self.ms_node,
                                    fs_info['nested_fs1']['url'],
                                    props='mount_point={0}'
                                    .format(file_system_name_1_updated))

        # Create and run deployment plan
        self.log("info", "Create/Run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Verify mount_point updates")
        self.verify_properties(node_fs_urls)

    @attr('all', 'revert', 'story216609', 'story216609_tc03')
    def test_03_p_remove_lvm_fs_with_nested_mnt_path(self):
        '''
        @tms_id: TORF-216609_tc03
        @tms_requirements_id: TORF-216609
        @tms_title: Remove a lvm fs with a nested mount path while also
        removing the mount path from an existing lvm fs with nested mount path
        @tms_description:
            Test to remove a lvm FS with nested mount path.
        @tms_test_steps:
            @step: Delete one of the newly added lvm file systems with the
            nested mount path.
            @result: Model item is changed to ForRemoval state
            @step: Remove the mount_point property from an lvm fs with a
            nested mount path.
            @result: the mount model item is set to Updated state
            @step: Create/Run plan
            @result: Plan is created and runs to completion
            @result: One lvm fs should be removed from model
            @result: One lvm fs should have its mount_point removed.
        @tms_test_precondition: Have 2 lvm fs applied with a nested mount
        paths.
        @tms_execution_type: Automated
        '''
        # Call create lvm fs method
        _, node_fs_urls, fs_info = self.create_new_fs()

        self.log("info", "Delete a file system's mount_point")
        # Remove mount point property from nested_fs1 fs
        self.execute_cli_update_cmd(self.ms_node,
                                    fs_info['nested_fs1']['url'],
                                    props='mount_point',
                                    action_del=True)
        # Remove nested_fs2 fs from model
        self.execute_cli_remove_cmd(self.ms_node,
                                    node_fs_urls[3])

        # Create and run deployment plan
        self.log("info", "Create/Run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)
