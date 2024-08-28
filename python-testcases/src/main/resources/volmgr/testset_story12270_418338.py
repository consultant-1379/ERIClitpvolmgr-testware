'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     April 2015
@author:    Carlos Branco, Stefan Ulian, Pallavi Kakade
@summary:   Integration
            Agile: STORY-12270
            Agile: STORY-418338
'''

from litp_generic_test import GenericTest, attr
import test_constants
from storage_utils import StorageUtils
from redhat_cmd_utils import RHCmdUtils


class Story12270(GenericTest):
    """
    As a LITP user I want to create an unmounted file system on an LV so
    that it can be mounted in a VM.

    STORY-418338 :: As a LITP user I want to  test the ability of volmanager to
    handle xfs file systems during an Initial Install.
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
        super(Story12270, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()
        self.ms_url = self.find(self.ms_node, '/', 'ms', exact_match=True)
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
        super(Story12270, self).tearDown()

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
        self.assertNotEqual([], stdout)
        return stdout

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

    def verify_fs(self, node, fs_url, size="",
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
        vol_details, _, _ =\
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
        for line in vol_details:
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
            fs_details, _, _ =\
            self.run_command(node, df_cmd, su_root=True)
            fs_found = False
            for line in fs_details:
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

    def manually_remove_snap(self, fs_url, snap_name=""):
        """
        Description:
            Function to manually remove snapshots
            from the nodes when LITP may've lost
            knowledge of them.
        Args:
            file_sys_dict (dict): All the vxvm file sys collected
            snap_name (str): tag assigned to the snapshot.
        """
        node = self.get_node_file_from_fs_url(fs_url)
        volume_name = \
        self.get_snap_lvscan_listing_name(fs_url, snap_name)
        lvremove_cmd = self.storage.get_lvremove_cmd(volume_name, "-f")
        self.run_command(node, lvremove_cmd, su_root=True)

    def get_snap_lvscan_listing_name(self, fs_url, snap_name):
        """
        Description:
            Function to retrieve the lvscan listing format of
            the supplied node file system.
        Args:
            fs_url(str): file system url on a node.
            snap_name(str): tag to snap name
        Returns:
            str.
        """
        snapshot_name = \
        self.get_snapshot_name_from_url(fs_url, 'lvm', snap_name)
        return "/dev/vg_root/{0}".format(snapshot_name)

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

    def verify_snaps_taken(self, node_fs_urls, negative_chk=False,
            is_ms=False):
        """
        Description:
            Function to verify in LVM that the desired
            snapshots have indeed been taken.
        Args:
            node_fs_urls(list): urls to f-s on nodes.
            negative_chk(bool): flag on whether to check for
                                snaps existance.
            is_ms(bool): verify on ms
        """
        lvs_cmd = self.storage.get_lvs_cmd()
        for fs_url in node_fs_urls:
            if is_ms:
                node = self.ms_node
            else:
                node = self.get_node_file_from_fs_url(fs_url)
            snap_name = self.get_snapshot_name_from_url(fs_url)
            stdout, _, _ = \
            self.run_command(node, lvs_cmd, su_root=True)
            if not negative_chk:
                self.assertTrue(self.is_text_in_list(snap_name, stdout))
            else:
                self.assertFalse(self.is_text_in_list(snap_name, stdout))

    def manually_remove_snap_ms(self, fs_url, snap_name=""):
        """
        Description:
            Function to manually remove snapshots
            from the nodes when LITP may've lost
            knowledge of them.
        Args:
            file_sys_dict (dict): All the vxvm file sys collected
            snap_name (str): tag assigned to the snapshot.
        """
        volume_name = \
        self.get_snap_lvscan_listing_name(fs_url, snap_name)
        lvremove_cmd = self.storage.get_lvremove_cmd(volume_name, "-f")
        self.run_command(self.ms_node, lvremove_cmd, su_root=True)

    def verify_properties(self, node_fs_urls):
        """
        Verifies mount_points and size properties
        """

        for fs_url in node_fs_urls:
            node = self.get_node_file_from_fs_url(fs_url)
            props = self.get_props_from_url(self.ms_node, fs_url)
            if "mount_point" in props:
                self.verify_fs(node, fs_url, props["size"],
                               props["mount_point"])
            else:
                self.verify_fs(node, fs_url, props["size"])

    def test_01_step01_to_03(self):
        """
        Description:
            Function to perform test_01 steps 01 to 03:
            1) Below and existing storage-profile of type lvm that is used on
               all of the nodes create 4 new file-systems.
               - fs_12270_1, fs_12270_2 with mount_point specified.
               - fs_12270_3, fs_12270_4 with no mount_point specified.
            2)  Create & run plan.
            3)  Verify.

        Args:
            vg_id: volume group
            node_fs_urls(list): urls to f-s on nodes.
            file_system_info(dict): f-s names and fs_urls
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
        self.get_all_vol_grps_from_storage_profile(derefd_url)
        vg_id = self.get_id_from_end_of_url(vol_grps[0])

        # CREATE THE NEW FILE SYSTEM BELOW THIS PROFILE
        self.log("info",
            "Create 4 file systems, three with mount point, one without")

        fss = {"fs1": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270_1",
                    },
               "fs2": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270_2"
                    },
               "fs3": {
                    "type": "ext4", "size": "20M",
                    "snap_size": "70"
                    },
               "fs4": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270_4"
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

    def create_xfs_fs(self, node_urls):
        """
        Description:
            1)  Remove existing snapshots.
            2)  Ensure all nodes are using same profiles.
            3)  Function to create 4 new xfs file-systems.
            4)  Create & run plan.

        Args:
            node_urls(list): node urls

        Returns:
            vg_id: volume group
            fss(dict): fs names and fs_urls
        """
        self.remove_all_snapshots(self.ms_node)
        derefd_urls = set()
        self.log("info",
                 "Ensure that all the nodes are using the same profile")
        for node in node_urls:
            derefd_url = self.deref_inherited_path(self.ms_node, node +
                                                   "/storage_profile")
            derefd_urls.add(derefd_url)

        self.assertTrue(len(derefd_urls) == 1,
                        "Storage profiles are different")
        derefd_url = derefd_urls.pop()
        vol_grps = \
            self.get_all_vol_grps_from_storage_profile(derefd_url)
        vg_id = self.get_id_from_end_of_url(vol_grps[0])

        self.log("info",
            "Create 4 file systems with mount point, size and snap size")

        fss = {"fs1_xfs": {
                    "type": "xfs",
                    "size": "20M",
                    "mount_point": "/fs_418338_1",
                    "snap_size": 100,
                    },
               "fs2_xfs": {
                    "type": "xfs",
                    "size": "20M",
                    "mount_point": "/fs_418338_2",
                    "snap_size": 100,
                    },
               "fs3_xfs": {
                    "type": "xfs",
                    "size": "20M",
                    "mount_point": "/fs_418338_3",
                    "snap_size": "70",
                    },
               "fs4_xfs": {
                    "type": "xfs",
                    "size": "20M",
                    "mount_point": "/fs_418338_4",
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

        return vg_id, fss

    @attr('all', 'revert', 'story12270', 'story12270_tc01')
    def test_01_p_create_lvm_no_mnt_pnt_on_node(self):
        '''
        @tms_id: litpcds_12270_tc01
        @tms_requirements_id: LITPCDS-12270
        @tms_title: Create lvm volume without mount_point
        @tms_description:
            To ensure that it is possible to create a file-system of
            type lvm which has no "mount_point" property specified on the
            managed nodes.
        @tms_test_steps:
            @step: Create two file systems with mount_point set, and two with
                  mount_point unset
            @result: file systems are created in the model
            @step: Create/Run plan
            @result: file system are created in the model
            @step: Update a file system's mount_point under /infrastructure
            @result: mount_point is updated
            @step: Delete a file system's mount_point
            @result: mount_point is deleted
            @step: Increase the size of both of those file-systems.
            @result: Sizes are increased in the model
            @step: Create/Run plan
            @result: Mount point of first file system has been updated
            @result: Mount point of second file system has been deleted
            @result: File system sizes are updated
            @step: Create a deployment snapshot
            @result: Snapshots are taken
            @step: manually remove a unmounted snapshot
            @result: snapshot is removed
            @step: litp remove/create snapshot
            @result: plans succeed
            @result: Snapshots are taken
            @step: Add a mount_point to an unmounted fs
            @result: Plan succeeds
            @result: Mount point is created
            @step: Create a dummy file in a fs
            @result: dummy file is created
            @step: restore snapshot
            @result: The previously unmounted fs is not mounted
            @step: Read the mount point
            @result: Mount point is added again
            @result: fs is mounted again
            @result: dummy file is not present
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        vg_id, node_fs_urls, fs_info = self.test_01_step01_to_03()
        vg_url = self.find_parent_path_from_item_type(self.ms_node,
            'volume-group', node_fs_urls[0])
        vg_name = self.get_props_from_url(self.ms_node, vg_url,
                                "volume_group_name")

        # remove remaining volumes after model cleanup
        for node in self.mn_nodes:
            self.run_commands_after_cleanup(node,
            ["/bin/umount /fs*",
             "/sbin/lvremove -f /dev/{0}/{1}_fs*".format(vg_name, vg_id),
             r"/bin/sed -i '\/dev\/{0}\/{1}_fs*/d' /etc/fstab".format(vg_name,
                                                                    vg_id)],
             su_root=True)

        self.log("info",
            "Update a file system's mount_point under /infrastructure")
        file_system_name_1_updated = "fs1_updated"
        self.execute_cli_update_cmd(self.ms_node, fs_info['fs1']['url'], \
                props='mount_point=/{0}'.format(file_system_name_1_updated))

        self.log("info", "Delete a file system's mount_point")
        self.execute_cli_update_cmd(self.ms_node, fs_info['fs2']['url'],
                                    props='mount_point', action_del=True)

        self.log("info", "Increase the size of both of those file-systems.")
        self.execute_cli_update_cmd(self.ms_node, fs_info['fs3']['url'],
                                    props='size=32M')
        self.execute_cli_update_cmd(self.ms_node, fs_info['fs4']['url'],
                                    props='size=32M')

        self.log("info", "Create/Run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Verify mount_point updates")
        self.verify_properties(node_fs_urls)

        self.log("info", "Verify new sizes")
        self.verify_properties(node_fs_urls)

        self.log("info", "Create a deployment snapshot")
        self.execute_and_wait_createsnapshot(self.ms_node)
        self.verify_snaps_taken(node_fs_urls)

        self.log("info", "Manually remove an unmounted snapshot")
        for node_url in self.node_urls:
            fs_url = "{0}/storage_profile/volume_groups/{1}" \
                     "/file_systems/{2}".format(node_url, vg_id, 'fs3')
            self.manually_remove_snap(fs_url)

        self.log("info", "litp Remove/Create snapshot")
        self.execute_and_wait_createsnapshot(self.ms_node)

        self.log("info", "Add a mount_point to an unmounted fs")
        file_system_name_3_mount = "fs3_mount"
        self.execute_cli_update_cmd(self.ms_node, fs_info['fs3']['url'], \
                    props='mount_point=/{0}'.format(file_system_name_3_mount))

        self.run_and_check_plan(self.ms_node,
                expected_plan_state=test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "create dummy.txt file inside /fs3_mount")
        file_path = self.join_paths("/{0}".format(file_system_name_3_mount),
                                    "dummy.txt")
        for node_url in self.node_urls:
            node = self.get_node_filename_from_url(self.ms_node, node_url)
            create_success = self.create_file_on_node(node, file_path,
                                                      ["", ""],
                                                      su_root=True)
            self.assertTrue(create_success, "File could not be created")

        self.log("info", "restore snapshot")
        self.execute_and_wait_restore_snapshot(self.ms_node)
        self.execute_and_wait_removesnapshot(self.ms_node)

        self.log("info", "verify fs3 is not mounted")
        for node_url in self.node_urls:
            fs_url = "{0}/storage_profile/volume_groups/{1}" \
                     "/file_systems/{2}".format(node_url, vg_id, 'fs3')
            node = self.get_node_filename_from_url(self.ms_node, node_url)
            size = self.get_props_from_url(self.ms_node, fs_url,
                filter_prop="size")
            self.verify_fs(node, fs_url, size,
                        file_system_name_3_mount, True)

        self.log("info", "Re-add the mount point")
        self.execute_cli_update_cmd(self.ms_node, fs_info['fs3']['url'], \
                props='mount_point=/{0}'.format(file_system_name_3_mount))

        self.run_and_check_plan(self.ms_node,
                expected_plan_state=test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Verify mount point is recreated")
        # VERIFY NEW MOUNTS CREATED
        for node_url in self.node_urls:
            fs_url = "{0}/storage_profile/volume_groups/{1}" \
                     "/file_systems/{2}".format(node_url, vg_id, 'fs3')
            node = self.get_node_filename_from_url(self.ms_node, node_url)
            size = self.get_props_from_url(self.ms_node, fs_url,
                filter_prop="size")
            self.verify_fs(node, fs_url, size, file_system_name_3_mount)
            self.log("info", "check dummy.txt is not present")
            self.assertFalse(self.remote_path_exists(node, file_path,
                                                     su_root=True))

    @attr('all', 'non-revert', 'story12270', 'story12270_tc02')
    def test_02_p_integration_tests_on_ms(self):
        '''
        @tms_id: litpcds_12270_tc02
        @tms_requirements_id: LITPCDS-12270
        @tms_title: Create lvm volume without mount_point
        @tms_description:
            To ensure that it is possible to create a file-system of
            type lvm which has no "mount_point" property specified on the
            ms.
        @tms_test_steps:
            @step: Create three file systems with mount_point set, and two with
                    mount_point unset
            @result: file systems are created in the model
            @step: Create/Run plan
            @result: file system are created in the model
            @step: Update a file system's mount_point under /infrastructure
            @result: mount_point is updated
            @step: Delete a file system's mount_point
            @result: mount_point is deleted
            @step: Increase the size of both of those file-systems.
            @result: Sizes are increased in the model
            @step: Create/Run plan
            @result: Mount point of first file system has been updated
            @result: Mount point of second file system has been deleted
            @result: File system sizes are updated
            @step: Create a deployment snapshot
            @result: Snapshots are taken
            @step: manually remove a unmounted snapshot
            @result: snapshot is removed
            @step: litp remove/create snapshot
            @result: plans succeed
            @result: Snapshots are taken
            @step: Add a mount_point to an unmounted fs
            @result: Plan succeeds
            @result: Mount point is created
            @step: Create a dummy file in a fs
            @result: dummy file is created
            @step: restore snapshot
            @result: The previously unmounted fs is not mounted
            @step: Read the mount point
            @result: Mount point is added again
            @result: fs is mounted again
            @result: dummy file is not present
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        self.log("info",
            "Create 4 file systems, three with mount point, one without")

        # Getting pvdisplay info
        pvdisplayinfo = self.get_pv_info_on_node(self.ms_node)

        # Getting UUID of root_vg
        disk_name = pvdisplayinfo.keys()[0].split('/')[-1]

        # Get UUID of root vg
        cmd = "/bin/ls -l /dev/disk/by-id | /bin/grep scsi"\
              " | /bin/grep {0} | ".format(disk_name) +\
              "awk -F ' ' '{ print $9 }' | sed s/scsi-3// | sed s/-part.//"

        stdout, _, _ = self.run_command(self.ms_node, cmd, su_root=True)
        # Removing the leading 3 (It is the id of the manufacture)
        uuid_root_vg = stdout[0]

        # GET SYSTEM PATH from plan of ms
        sys_path_url = \
        self.deref_inherited_path(self.ms_node, "/ms/system")

        # Create a disk device:

        disk_url = sys_path_url + "/disks/d1"
        disk_props = {'name': 'sda',
                      'size': '1168G',
                      'bootable': 'true',
                      'uuid': uuid_root_vg}

        props = "name='{0}' size='{1}' bootable='{2}' uuid='{3}'".\
                format(disk_props['name'], disk_props['size'],
                       disk_props['bootable'], disk_props['uuid'])

        self.execute_cli_create_cmd(self.ms_node, disk_url,
                                    "disk", props, add_to_cleanup=False)

        # Create the storage profile:

        profile_path = self.find(self.ms_node, "/infrastructure",
                                 "storage-profile-base", False)[0]
        sp_url = profile_path + "/profile_12270"
        self.execute_cli_create_cmd(self.ms_node, sp_url,
                                    "storage-profile", add_to_cleanup=False)

        # Add the vg_root to the model
        # Although vg_root exists in Unix it is not in LITP model.
        # Create vg_root, and then create a new file system under it
        vg_url = sp_url + "/volume_groups/vg_root"
        vg_props = {"name": "vg_root",
                     "url": vg_url}
        props = "volume_group_name='{0}'".format(vg_props['name'])
        self.execute_cli_create_cmd(self.ms_node, vg_url,
                                    "volume-group", props,
                                    add_to_cleanup=False)

        # Define the physical device(s) to be added
        ph_url = vg_url + "/physical_devices/pd12270"
        ph_props = {'device_name': 'sda'}
        props = "device_name='{0}'"\
                "".format(ph_props['device_name'])
        self.execute_cli_create_cmd(self.ms_node, ph_url,
                                    "physical-device", props,
                                    add_to_cleanup=False)

        # Inherit the configuration to the management server:
        self.execute_cli_inherit_cmd(self.ms_node,
                                     "/ms/storage_profile",
                                     sp_url, add_to_cleanup=False)

        fss = {"fs1": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270_1",
                    "snap_size": 100,
                    },
               "fs2": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270_2",
                    "snap_size": 100,
                    },
               "fs3": {
                    "type": "ext4",
                    "size": "20M",
                    "snap_size": "70",
                    },
               "fs4": {
                    "type": "ext4",
                    "size": "20M"
                    },
               "fs5": {
                    "type": "ext4",
                    "size": "20M",
                    "mount_point": "/fs_12270_5"
                    }

               }

        for fsystem, prop_dict in fss.iteritems():
            fs_url = "{0}/file_systems/{1}".format(vg_url,
                                                fsystem)
            props = " ".join("{0}={1}".format(key, value) for (key, value) in
              prop_dict.iteritems())
            fss[fsystem]['url'] = fs_url
            self.execute_cli_create_cmd(self.ms_node,
                                    fs_url, "file-system",
                                    props, add_to_cleanup=False)

        # Create, run plan and wait to finish
        self.run_and_check_plan(self.ms_node,
                expected_plan_state=test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        # verify: file system has been created,
        #         mount point created when applicable,
        #         the file system mounted (when a mount point was created)
        #         volume has been created.
        for fsystem, prop_dict in fss.iteritems():
            if prop_dict.get('mount_point'):
                self.verify_fs(self.ms_node, prop_dict['url'],
                       prop_dict["size"],
                       prop_dict["mount_point"], is_ms=True)
            else:
                self.verify_fs(self.ms_node, prop_dict['url'],
                       prop_dict["size"],
                       is_ms=True)

        self.log("info",
            "Update a file system's mount_point under /infrastructure")
        fss['fs1']['mount_point'] = 'test_12270_1_updated'

        self.execute_cli_update_cmd(self.ms_node, fss['fs1']['url'],
                                    props='mount_point=/{0}'.format(
                                        fss['fs1']['mount_point']))

        self.log("info", "Delete a file system's mount_point")
        self.execute_cli_update_cmd(self.ms_node, fss['fs2']['url'],
                                    props='mount_point', action_del=True)

        self.log("info", "Increase the size of both of those file-systems.")
        fss['fs3']["size"] = "32M"
        self.execute_cli_update_cmd(self.ms_node, fss['fs3']["url"],
                props='size={0}'.format(fss['fs3']["size"]))

        fss['fs5']["size"] = "32M"
        self.execute_cli_update_cmd(self.ms_node, fss['fs5']["url"],
                props='size={0}'.format(fss['fs5']["size"]))

        self.log("info", "Create/Run plan")
        self.run_and_check_plan(self.ms_node,
                expected_plan_state=test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Verify mount_point updates")
        self.verify_fs(self.ms_node, fss['fs1']['url'], fss['fs1']["size"],
            fss['fs1']["mount_point"], is_ms=True)

        self.verify_fs(self.ms_node, fss['fs2']['url'], fss['fs2']["size"],
            fss['fs2']["mount_point"], is_ms=True, negative_chk=True)

        self.log("info", "Verify new sizes")
        self.verify_fs(self.ms_node, fss['fs3']['url'], fss['fs3']["size"],
                is_ms=True)
        self.verify_fs(self.ms_node, fss['fs5']['url'], fss['fs5']["size"],
                       fss['fs5']["mount_point"], is_ms=True)

        self.log("info", "Create a deployment snapshot")
        self.execute_and_wait_createsnapshot(self.ms_node)
        ms_fs_urls = [fsystem['url'] for fsystem in fss.values()]
        self.verify_snaps_taken(ms_fs_urls, is_ms=True)

        self.log("info", "Manually remove an unmounted snapshot")
        self.manually_remove_snap_ms(fss['fs3']['url'])

        self.log("info", "litp Remove/Create snapshot")
        self.execute_and_wait_createsnapshot(self.ms_node)

        self.log("info", "Add a mount_point to an unmounted fs")
        fss['fs4']['mount_point'] = 'test_12270_4_updated'
        self.execute_cli_update_cmd(self.ms_node,
                                    fss['fs4']['url'],
                                    props='mount_point=/{0}'.format(
                                        fss['fs4']['mount_point']))

        self.run_and_check_plan(self.ms_node,
                expected_plan_state=test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.verify_fs(self.ms_node, fss['fs4']['url'], fss['fs4']["size"],
                       fss['fs4']["mount_point"], is_ms=True)

        self.log("info", "create dummy.txt file inside /fs4_mount")
        file_path = self.join_paths("/{0}".format(fss['fs4']['mount_point']),
                                    "dummy.txt")
        create_success = self.create_file_on_node(self.ms_node, file_path,
                                               ["", ""],
                                               su_root=True)
        self.assertTrue(create_success, "File could not be created")

        self.log("info", "restore snapshot")
        self.execute_and_wait_restore_snapshot(self.ms_node)
        self.execute_and_wait_removesnapshot(self.ms_node)

        self.log("info", "verify fs4 is not mounted")
        self.verify_fs(self.ms_node, fss['fs4']['url'], fss['fs4']["size"],
            fss['fs4']["mount_point"], negative_chk=True, is_ms=True)

        self.log("info", "Re-add the mount point")
        # Update fs_12270_4 fileystem with the same mount point.
        self.execute_cli_update_cmd(self.ms_node,
                                    fss['fs4']['url'],
                                    props='mount_point=/{0}'.format(
                                        fss['fs4']['mount_point']))

        self.run_and_check_plan(self.ms_node,
                expected_plan_state=test_constants.PLAN_COMPLETE,
                plan_timeout_mins=30)

        self.log("info", "Verify mount point is recreated")
        # VERIFY NEW MOUNT CREATED for fs_4
        self.verify_fs(self.ms_node, fss['fs4']['url'], fss['fs4']["size"],
                       fss['fs4']["mount_point"], is_ms=True)

        self.log("info", "check dummy.txt is not present")
        self.assertFalse(self.remote_path_exists(self.ms_node, file_path,
                                                        su_root=True))

    @attr('all', 'revert', 'story418338', 'story418338_tc04_tc05_tc06')
    def test_03_p_create_xfs_fs_on_node(self):
        '''
        @tms_id: torf_418338_tc04_tc05_tc06
        @tms_requirements_id: TORF-418338
        @tms_title: Create xfs file system with specified properties
             on the managed nodes.
        @tms_description:
            To ensure that it is possible to create a file-system of
            type xfs with specified properties on the managed nodes.
        @tms_test_steps:
            @step: Create four xfs file systems with mount_point, size and
                   snap-size specified.
            @result: file systems are created
            @step: Verify the file systems are created
            @result: File systems are created
            @step: Update a file system's mount_point under /infrastructure
            @result: mount_point is updated
            @step: Delete a file system's mount_point
            @result: mount_point is deleted
            @step: Increase the size of the file-system.
            @result: Size is increased in the deployment
            @step: Create/Run plan
            @result: created and run plan
            @step: Verify the updated mount_point and the sizes
            @result: Mount_point and the sizes are updated
            @step: Perform read/write operation on xfs File
            @result: Successful read/write operation on xfs File
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        vg_id, fss = self.create_xfs_fs(self.node_urls)
        self.log("info", "Ensure file systems have been created")
        node_fs_urls = []

        for node_url in self.node_urls:
            for fsystem in fss:
                node_fs_urls.append("{0}/storage_profile/"
                      "volume_groups/{1}/file_systems/{2}".format(node_url,
                                                       vg_id, fsystem))
        self.verify_properties(node_fs_urls)
        vg_url = self.find_parent_path_from_item_type(self.ms_node,
                                'volume-group', node_fs_urls[0])
        vg_name = self.get_props_from_url(self.ms_node, vg_url,
                                          "volume_group_name")

        # remove remaining volumes after model cleanup
        for node in self.mn_nodes:
            self.run_commands_after_cleanup(node,
                  ["{0} /fs*".format(test_constants.UMOUNT_PATH),
                  "/sbin/lvremove -f /dev/{0}/{1}_fs*".format(vg_name,
                    vg_id), "{0} -i '\\/dev\\/{1}\\/{2}_fs*/d' "
                  r"/etc/fstab".format(test_constants.SED_PATH, vg_name,
                        vg_id)], su_root=True)

        self.log("info",
                 "Update a file system's mount_point under /infrastructure")
        file_system_name_1_updated = "fs1_xfs_updated"
        self.execute_cli_update_cmd(self.ms_node, fss['fs1_xfs']['url'],
                props='mount_point=/{0}'.format(file_system_name_1_updated))

        self.log("info", "Delete a file system's mount_point")
        self.execute_cli_update_cmd(self.ms_node, fss['fs2_xfs']['url'],
                         props='mount_point', action_del=True)

        self.log("info", "Increase the size of the file-systems.")
        self.execute_cli_update_cmd(self.ms_node, fss['fs4_xfs']['url'],
                                    props='size=32M')

        self.log("info", "Create/Run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                                plan_timeout_mins=30)

        self.log("info", "Verify the updated mount_point and the "
                         "new sizes")
        self.verify_properties(node_fs_urls)
        self.log("info",
                 "Perform read/write operation on xfs file system type"
                 " for the nodes ")
        write_cmd = "dd if=/dev/zero of=/testfile.txt bs=1G count=7"
        read_cmd = "dd if=/testfile.txt of=/dev/zero bs=1G count=7"
        for node in self.mn_nodes:
            node_write_cmd, _, _ = self.run_command(node, write_cmd,
                                       su_root=True, su_timeout_secs=300)
            self.assertNotEqual([], node_write_cmd, "Write command Failed")
            node_read_cmd, _, _ = self.run_command(node, read_cmd,
                                        su_root=True, su_timeout_secs=300)
            self.assertNotEqual([], node_read_cmd, "Read command Failed")

    @attr('all', 'non-revert', 'story418338', 'story418338_tc01_tc02_tc03')
    def test_04_p_create_xfs_fs_on_ms(self):
        '''
        @tms_id: torf_418338_tc01_tc02_tc03
        @tms_requirements_id: TORF-418338
        @tms_title: Create xfs file system with specified properties
               on the MS.
        @tms_description:
            To ensure that it is possible to create a file-system of
               type xfs with specified properties on the MS.
        @tms_test_steps:
            @step: Create four xfs file systems with mount_point, size and
                   snap-size specified.
            @result: file systems are created
            @step: Verify the file systems are created
            @result: File systems are created
            @step: Update a file system's mount_point under /infrastructure
            @result: mount_point is updated
            @step: Delete a file system's mount_point
            @result: mount_point is deleted
            @step: Increase the size of the file-system.
            @result: Size is increased in the deployment
            @step: Create/Run plan
            @result: created and run plan
            @step: Verify the updated mount_point
            @result: Mount_point are updated
            @step: Verify the updated sizes
            @result: Sizes are updated
            @step: Perform read/write operation on xfs File
            @result: Successful read/write operation on xfs File
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        fss = self.create_xfs_fs(self.ms_url)[1]

        # verify: file system has been created,
        #         mount point created when applicable,
        #         the file system mounted (when a mount point was created)
        #         volume has been created.
        self.log("info",
               "Verify the Deployement for new file systems ")
        for _, prop_dict in fss.iteritems():
            if prop_dict.get('mount_point'):
                self.verify_fs(self.ms_node, prop_dict['url'],
                    prop_dict["size"], prop_dict["mount_point"],
                                is_ms=True)
            else:
                self.verify_fs(self.ms_node, prop_dict['url'],
                                prop_dict["size"], is_ms=True)

        self.log("info",
              "Update a file system's mount_point under /infrastructure")
        fss['fs1_xfs']['mount_point'] = 'test_418338_1_updated'
        self.execute_cli_update_cmd(self.ms_node, fss['fs1_xfs']['url'],
             props='mount_point=/{0}'.format(fss['fs1_xfs']['mount_point']))

        self.log("info", "Delete a file system's mount_point")
        self.execute_cli_update_cmd(self.ms_node, fss['fs2_xfs']['url'],
                                  props='mount_point', action_del=True)

        self.log("info", "Increase the size of the file-systems.")
        fss['fs3_xfs']["size"] = "32M"
        self.execute_cli_update_cmd(self.ms_node, fss['fs3_xfs']["url"],
                     props='size={0}'.format(fss['fs3_xfs']["size"]))

        self.log("info", "Create/Run plan")
        self.run_and_check_plan(self.ms_node,
              expected_plan_state=test_constants.PLAN_COMPLETE,
                                    plan_timeout_mins=30)

        self.log("info", "Verify mount_point updates")
        self.verify_fs(self.ms_node, fss['fs1_xfs']['url'],
            fss['fs1_xfs']["size"], fss['fs1_xfs']["mount_point"],
                    is_ms=True)
        self.verify_fs(self.ms_node, fss['fs2_xfs']['url'],
              fss['fs2_xfs']["size"], fss['fs2_xfs']["mount_point"],
                           is_ms=True, negative_chk=True)

        self.log("info", "Verify new sizes")
        self.verify_fs(self.ms_node, fss['fs3_xfs']['url'],
            fss['fs3_xfs']["size"], fss['fs3_xfs']["mount_point"],
                    is_ms=True)

        self.log("info",
                     "Perform read/write operation on xfs "
                     "file system type for the MS  ")
        # write operation on xfs File
        write_cmd = "dd if=/dev/zero of=/testfile.txt bs=1G count=7"
        node_write_cmd, _, _ = self.run_command(self.ms_node, write_cmd,
                                                    su_root=True)
        self.assertNotEqual([], node_write_cmd, "Read command Failed "
                                                    "on MS")

        # read operation on xfs File
        read_cmd = "dd if=/testfile.txt of=/dev/zero bs=1G count=7"
        node_read_cmd, _, _ = self.run_command(self.ms_node, read_cmd,
                                                   su_root=True)
        self.assertNotEqual([], node_read_cmd, "Read command Failed "
                                                  "on MS")
