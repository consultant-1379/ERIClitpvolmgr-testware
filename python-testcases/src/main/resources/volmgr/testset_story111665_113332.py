'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     June 2016
@author:    Stefan Ulian, Pallavi Kakade
@summary:   Integration
            Agile: STORY-111665
            Agile: STORY-418338
'''

from litp_generic_test import GenericTest, attr
import test_constants
from storage_utils import StorageUtils
from redhat_cmd_utils import RHCmdUtils
import time
import os


class Story111665(GenericTest):
    """
    As a LITP user, I want to model the LVM file systems in the root volume
    group which are defined in the MS kickstart so that I can resize them
    dynamically.
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
        super(Story111665, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.storage = StorageUtils()
        self.rhcmd = RHCmdUtils()

        self.vg_root_url = self.get_root_volume_group_url(
                "/ms/storage_profile")

        # TORF-418338 - Create a default root file system of
        #               type 'xfs' in the LITP model
        # converting /root file system to 'xfs' type
        self.ks_fss = {'var': {'type': 'xfs',
                       'mount_point': '/var',
                       'size': '10G',
                       'snap_size': '100',
                       'backup_snap_size': '10',
                       'url': self.vg_root_url + "/file_systems/var"},
                'var_log': {'type': 'xfs',
                       'mount_point': '/var/log',
                       'size': '20G',
                       'snap_size': '100',
                       'backup_snap_size': '10',
                       'url': self.vg_root_url + "/file_systems/var_log"},
                'var_www': {'type': 'xfs',
                    'mount_point': '/var/www',
                    'size': '140G',
                    'snap_size': '100',
                    'backup_snap_size': '10',
                    'url': self.vg_root_url + "/file_systems/var_www"},
                'home': {'type': 'xfs',
                     'mount_point': '/home',
                     'size': '12G',
                     'snap_size': '100',
                     'backup_snap_size': '10',
                     'url': self.vg_root_url + "/file_systems/home"},
                'root': {'type': 'xfs',
                     'mount_point': '/',
                     'size': '70G',
                     'snap_size': '100',
                     'backup_snap_size': '10',
                    'url': self.vg_root_url + "/file_systems/root"},
                'software': {'type': 'xfs',
                     'mount_point': '/software',
                     'size': '150G',
                     'snap_size': '1',
                     'backup_snap_size': '10',
                     'url': self.vg_root_url + "/file_systems/software"}}

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
        super(Story111665, self).tearDown()

    def _verify_grub_dir(self, dir_contents):
        """ Verify grub_dir """
        out = self._get_grub_dir_contents()
        self.assertEqual(dir_contents, out)

    def _get_grub_dir_contents(self):
        """ Get the Grub directory contents """
        grub_dir, _ = os.path.split(test_constants.GRUB_CONFIG_FILE)
        cmd = "/bin/ls -l {0}".format(grub_dir)
        out, _, _ = self.run_command(self.ms_node, cmd, su_root=True,
                default_asserts=True)
        return out

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

    def get_snapshot_name_from_url(self, url, vol_driver='lvm', name=''):
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
        fs_lv = self.get_volume_name(url)

        if vol_driver == 'lvm':
            vg_id = str_list[index + 1]
            if name != '':
                if vg_id == 'vg_root':
                    return "L_{0}_{1}".format(fs_lv, name)
                else:
                    return "L_{0}_{1}_{2}".format(vg_id, fs_id, name)
            else:
                if vg_id == 'vg_root':
                    return "L_{0}_".format(fs_lv)
                else:
                    return "L_{0}_{1}_".format(vg_id, fs_id)
        else:
            if name != '':
                return "L_{0}_{1}".format(fs_id, name)

            return "L_{0}_".format(fs_id)

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
        vol_name = ''
        ks_mps = ['/', 'swap', '/home', '/var', '/var/log', '/var/www',
                  '/software']
        vg_url = \
            self.find_parent_path_from_item_type(self.ms_node, 'volume-group',
                                                fs_url)
        fs_mp = self.get_props_from_url(self.ms_node, fs_url, 'mount_point')
        vg_id = self.get_id_from_end_of_url(vg_url)

        if vg_id == 'vg_root' and fs_mp in ks_mps:
            if fs_mp == '/':
                vol_name = "lv_root"
            else:
                vol_name = "lv{0}".format("_".join(fs_mp.split('/')))
        else:
            fs_id = self.get_id_from_end_of_url(fs_url)
            vol_name = "{0}_{1}".format(vg_id, fs_id)

        return vol_name

    def get_root_volume_group_url(self, url):
        '''
        Description:
            Function to compile and return the root volume group url of the
            supplied storage profile.
        Args:
            sp_url (str): url to a storage profile.
        Returns:
            str.
        '''
        derefd_urls = []
        derefd_url = \
        self.deref_inherited_path(self.ms_node, url)
        derefd_urls.append(derefd_url)
        derefd_url = derefd_urls[0]
        vg_root_url = \
        self.get_all_vol_grps_from_storage_profile(derefd_url)[0]

        return vg_root_url

    def verify(self, node, fs_url, size="",
               mount_point="", negative_chk=False):
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
        volume_name = self.get_volume_name(fs_url)
        vg_url = self.find_parent_path_from_item_type(self.ms_node,
                                                      'volume-group',
                                                      fs_url)
        vg_name = \
        self.get_props_from_url(self.ms_node, vg_url,
                                "volume_group_name")

        # ENSURE THE VOLUME HAS BEEN CREATED
        vol_found = False
        vol_size_correct = False
        for line in stdout:
            split_lines = line.split(" ")
            tidied_list = [x for x in split_lines if x != '']
            if volume_name == tidied_list[0]:
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
                self.assertTrue(found)
                self.assertTrue(fs_found)

    def verify_snaps_taken_ms(self, node_fs_urls, negative_chk=False):
        """
        Description:
            Function to verify in LVM that the desired
            snapshots have indeed been taken.
        Args:
            node_fs_urls(list): urls to f-s on nodes.
            negative_chk(bool): flag on whether to check for
                                snaps existance.
        """
        lvs_cmd = self.storage.get_lvs_cmd()
        for fs_url in node_fs_urls:
            snap_name = self.get_snapshot_name_from_url(fs_url)
            stdout, _, _ = \
            self.run_command(self.ms_node, lvs_cmd, su_root=True)
            if not negative_chk:
                self.assertTrue(self.is_text_in_list(snap_name, stdout))
            else:
                self.assertFalse(self.is_text_in_list(snap_name, stdout))

    def _verify_snap_sizes(self, ks_fss, snap_name):
        """
        Verifies the size of modelled lvm file systems
        """

        fsystem = self.get_lv_info_on_node(self.ms_node)

        for modelled_fs in ks_fss.values():
            snapshot_name = self.get_snapshot_name_from_url(modelled_fs['url'],
                    name=snap_name)
            snap_details = fsystem[snapshot_name]
            actual_size = float(snap_details['COW_TABLE_SIZE_MB'])

            if modelled_fs.get('backup_snap_size'):
                expected_size = float(modelled_fs['backup_snap_size']) *\
                        0.01 *\
                self.sto.convert_size_to_megabytes(modelled_fs['size'])
            else:
                expected_size = float(modelled_fs['snap_size']) *\
                    0.01 * self.sto.convert_size_to_megabytes(
                            modelled_fs['size'])

            error = abs((actual_size - expected_size) /\
                float(max(actual_size, expected_size)))
            self.assertTrue(error <= 0.05)

    @attr('all', 'non-revert', 'story111665', 'story111665_tc03')
    def test_03_p_create_depl_snaps_some_ks_fss_modeled(self):
        '''
        @tms_id: TORF_111665_tc03
        @tms_requirements_id: TORF-111665
        @tms_title: create depl snaps some kickstart fss modeled
        @tms_description:
            This test will verify that deployment snapshots can be
            successfully created when some kickstart filesystems are modeled.
        @tms_test_steps:
            @step: Remove all snapshots.
            @result: All snapshots were removed.
            @step: Model some of the kickstart file systems ('/var',
                   '/var/log', '/var/www').
            @result: Successfully modeled /var, /var/log ,/var/www kickstart
                     file systems.
            @step: Create/Run LITP Plan.
            @result: Successfully created and run LITP Plan.
            @step: Check kickstart filesystem sizes and mount points.
            @result: kickstart filesystems have the same size as specified in
                     the model.
            @result: kickstart filesystems are mounted.
            @step: Run create another snapshot.
            @result: create_snapshot runs successfully.
            @step: Check snapshots were created for /var, /var/log/,
                   /var/www kickstart filesystems.
            @result: Snapshots were created for /var, /var/log, /var/www
                   kickstart filesystems.
        @tms_test_precondition: This test unit has a dependency on
                                testset_story12270_418338.py which must pass
                                before this test can be run successfully.
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        self.log("info", "Model /home, /var/log, /var/www kickstart "
                         "file systems")
        for ks_fs, ks_fs_props in self.ks_fss.iteritems():
            if ks_fs in ['var', 'var_log', 'var_www']:
                props = "type='{0}' mount_point='{1}' size='{2}' " \
                    "snap_size='{3}' backup_snap_size='{4}'".format(
                                             ks_fs_props['type'],
                                             ks_fs_props['mount_point'],
                                             ks_fs_props['size'],
                                             ks_fs_props['snap_size'],
                                             ks_fs_props['backup_snap_size'],
                                             )
                self.execute_cli_create_cmd(self.ms_node, ks_fs_props['url'],
                                            "file-system", props,
                                            add_to_cleanup=False)

        self.log("info", "Create/Run LITP Plan.")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                                plan_timeout_mins=30)

        self.log("info", "Check /home, /var/log, /var/www "
                         "kickstart fileystem sizes and mount points.")
        for ks_fs, ks_fs_props in self.ks_fss.iteritems():
            if ks_fs in ['var', 'var_log', 'var_www']:
                self.verify(self.ms_node, ks_fs_props['url'],
                                      ks_fs_props['size'],
                                      ks_fs_props["mount_point"])

        self.log("info", "Run create snapshot.")
        self.execute_and_wait_createsnapshot(self.ms_node,
                                             add_to_cleanup=False)

        self.log("info", "Check snapshots were created for /var,"
                         "/var/log, /var/www kickstart filesystems.")
        ms_ks_fs_urls = [self.ks_fss['var']['url'],
                         self.ks_fss['var_log']['url'],
                         self.ks_fss['var_www']['url']]
        self.verify_snaps_taken_ms(ms_ks_fs_urls)

    @attr('all', 'non-revert', 'story111665', 'story418338',
          'story111665_tc04', 'story418338_tc01')
    def test_04_p_create_depl_snaps_all_ks_fss_modeled(self):
        '''
        @tms_id: TORF_111665_tc04, TORF_418338_tc01
        @tms_requirements_id: TORF-111665, TORF-418338
        @tms_title: create deployment snaps all kickstart fss modeled
        @tms_description:
            Verify that deployment snapshots can be successfully created when
            all kickstart filesystems are modeled.
        @tms_test_steps:
            @step: Remove all snapshots.
            @result: All snapshots were removed.
            @step: Model the remaining kickstart file systems ('/var',
                '/var/log', '/var/www', '/software', '/home' and '/' ).
            @result: Successfully modeled the remaining kickstart filesystems.
            @step: Create a root file system of type 'xfs' in the LITP model
            @result: Root file system of type 'xfs' is created successfully.
            @step: Create/Run Plan.
            @result: Successfully created and run the LITP Plan.
            @step: Check all kickstart fileystem sizes and mount points.
            @result: All kickstart filesystems sizes are correct and mounted.
            @step: Run create_snapshot.
            @result: Snapshots are created for all kickstart filesystems.
        @tms_test_precondition: This test unit has a dependency on$
                                testset_story12270_418338.py which must pass$
                                before this test can be run successfully.
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        self.log("info", "Model the remaining kickstart file systems: "
                         "/home, /root, /software.")
        for ks_fs, ks_fs_props in self.ks_fss.iteritems():
            if ks_fs in ['home', 'root', 'software']:
                props = "type='{0}' mount_point='{1}' size='{2}' " \
                    "snap_size='{3}' backup_snap_size='{4}'".format(
                                             ks_fs_props['type'],
                                             ks_fs_props['mount_point'],
                                             ks_fs_props['size'],
                                             ks_fs_props['snap_size'],
                                             ks_fs_props['backup_snap_size'])
                self.execute_cli_create_cmd(self.ms_node, ks_fs_props['url'],
                                            "file-system", props,
                                            add_to_cleanup=False)

        self.log("info", "Create/Run Plan.")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                                plan_timeout_mins=30)

        self.log("info", "Check all kickstart fileystem sizes and"
                        "mount points.")
        for ks_fs_props in self.ks_fss.itervalues():
            self.verify(self.ms_node, ks_fs_props['url'],
                                      ks_fs_props['size'],
                                      ks_fs_props["mount_point"])

        self.log("info", "Run create_snapshot.")
        self.execute_and_wait_createsnapshot(self.ms_node,
                                             add_to_cleanup=False)
        self.log('info', "Check deployment snapshots were created for "
                         "modeled kickstart filesystems.")
        ms_ks_fs_urls = [self.ks_fss['var']['url'],
                         self.ks_fss['var_log']['url'],
                         self.ks_fss['var_www']['url'],
                         self.ks_fss['home']['url'],
                         self.ks_fss['software']['url']]
        self.verify_snaps_taken_ms(ms_ks_fs_urls)

    @attr('all', 'non-revert', 'story111665', 'story111665_tc05')
    def test_05_p_model_new_fss_not_in_ks(self):
        '''
        @tms_id: TORF_111665_tc05
        @tms_requirements_id: TORF-111665
        @tms_title: model non-kickstart filesystems
        @tms_description:
            This test will verify that new non-kickstart filesystems can be
            modeled in the root volume group.
        @tms_test_steps:
            @step: Remove all snapshots.
            @result: All snapshots were removed.
            @step: Model new non-kickstart filesystems (/var/lib/libvirt,
                  /var/lib/mysql) inside root volume group.
            @result: Successfully modeled /var/lib/libvirt , /var/lib/mysql.
            @step: Create/Run Plan.
            @result: Successfully created and run the plan.
            @step: Check that /var/lib/libvirt, /var/lib/mysql filesystems
                      were created with the correct size and mount point.
            @result: The above check was successful.
            @step: Run create_snapshot.
            @result: Create_snapshot runs successfully.
            @result: Snapshots were created for the non-kickstart filesystems
        @tms_test_precondition: This test unit has a dependency on$
                                testset_story12270_418338.py which must pass$
                                before this test can be run successfully.
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        vg_root_url = self.get_root_volume_group_url("/ms/storage_profile")

        non_ks_fss = {'libvirt': {'type': 'ext4',
                               'mount_point': '/var/lib/libvirt',
                               'size': '5G',
                               'snap_size': '1',
                               'url': vg_root_url + "/file_systems/libvirt"},
                        'mysql': {'type': 'ext4',
                               'mount_point': '/var/lib/mysql',
                               'size': '2G',
                               'snap_size': '1',
                               'url': vg_root_url + "/file_systems/mysql"}}

        self.log("info", "Model new non-kickstart filesystems "
                         "(/var/lib/libvirt, /var/lib/mysql) inside root "
                         "volume group.")
        for non_ks_fs_props in non_ks_fss.itervalues():
            props = "type='{0}' mount_point='{1}' size='{2}' " \
                "snap_size='{3}'".format(non_ks_fs_props['type'],
                                         non_ks_fs_props['mount_point'],
                                         non_ks_fs_props['size'],
                                         non_ks_fs_props['snap_size'])
            self.execute_cli_create_cmd(self.ms_node, non_ks_fs_props['url'],
                                        "file-system", props,
                                        add_to_cleanup=False)

        self.log("info", "Create/Run Plan.")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                                plan_timeout_mins=30)

        self.log("info", "Check that /var/lib/libvirt, /var/lib/mysql"
                 " non-kickstart filesystems were created  with the correct "
                 "size and mount point.")
        for non_ks_fs_props in non_ks_fss.itervalues():
            self.verify(self.ms_node, non_ks_fs_props['url'],
                                      non_ks_fs_props['size'],
                                      non_ks_fs_props["mount_point"])

        self.log("info", "Run create_snapshot.")
        self.execute_and_wait_createsnapshot(self.ms_node,
                                             add_to_cleanup=False)

        self.log("info", "Check that deployment snapshots were created for "
                         "/var/lib/libvirt, /var/lib/mysql non-kickstart "
                         "filesystems.")
        ms_fs_urls = [non_ks_fss['libvirt']['url'],
                      non_ks_fss['mysql']['url']]
        self.verify_snaps_taken_ms(ms_fs_urls)

    @attr('all', 'non-revert', 'story111665', 'story111665_tc06')
    def test_06_p_create_named_snaps_all_ks_fss_modeled(self):
        '''
        @tms_id: TORF_111665_tc06
        @tms_requirements_id: TORF-111665
        @tms_title: create named snaps all kickstart filesystems modeled
        @tms_description:
            This test will verify that named snapshots can be successfully
            created when all kickstart filesystems were modeled.
        @tms_test_steps:
            @step: Remove all snapshots.
            @result: All snapshots were removed.
            @step: Create a named snapshot.
            @result: Snapshot plan succeeds.
            @result: Named Snapshots are created for all kickstart
                     filesystems.
            @result: Grub is backed up.
            @result: Timestamp is the current time.
            @result: Snapshots have the correct name and size.
            @step: Do an xml export of the model.
            @result: Model exported.
            @result: Named snapshots are present in the xml file.
            @step: Remove the named snapshots.
            @result: Named snapshots were removed.
        @tms_test_precondition: This test unit has a dependency on$
                                testset_story12270_418338.py which must pass$
                                before this test can be run successfully.
        @tms_execution_type: Automated
        '''
        snap_name = "test_06"
        snap_path = "/snapshots/" + snap_name

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node)

        boot_dir_contents = self._get_grub_dir_contents()

        self.log('info', 'Create a named snapshot')
        self.execute_and_wait_createsnapshot(self.ms_node,
            args="--name {0}".format(snap_name),
            add_to_cleanup=False)

        # Verify that there is a new 'named' snapshot item.
        self.assertTrue(self.is_snapshot_item_present(self.ms_node, snap_name))

        # Verify that the snapshot item is Applied.
        self.assertEqual("Applied", self.get_item_state(self.ms_node,
            snap_path))

        # Verify that the time-stamp is the current time.
        props_dict = self.get_props_from_url(self.ms_node, snap_path)
        ss_time = float(props_dict['timestamp'])
        curr_time = time.time()
        self.log("info", "Snapshot time is: {0}".format(ss_time))
        self.log("info", "Current time is : {0}".format(curr_time))
        self.assertTrue((curr_time - 180) < ss_time < (curr_time + 180),
                        "Timestamp isn't within 3 minutes of current time")

        # Verify the snapshot sizes
        self._verify_snap_sizes(self.ks_fss, snap_name)

        # Verify that there is no new grub backup.
        self._verify_grub_dir(boot_dir_contents)

        self.log('info', 'Do an xml export of the model')
        stdout, _, _ = self.execute_cli_export_cmd(self.ms_node, "/")

        #  Verify that the snapshot item is not being exported
        self.assertFalse(self.is_text_in_list(snap_name, stdout))

        self.log('info', 'Remove the named snapshot')
        self.execute_and_wait_removesnapshot(self.ms_node,
            args='--name {0}'.format(snap_name))

        stdout, _, _ = self.execute_cli_showplan_cmd(self.ms_node)

        # Check that the snapshots have been removed
        fss = self.get_all_volumes(self.ms_node)

        for filesys in fss:
            vx_disk_node = self.get_vx_disk_node(self.ms_node,
                    disk_group=filesys["volume_group_name"])

            if filesys["snap_size"] != '0':
                cmd = self.storage.get_vxsnap_cmd(filesys['volume_group_name'],
                                                  grep_args="L_")
                out, err, _ = self.run_command(vx_disk_node,
                                               cmd,
                                               su_root=True)
                expect_ss_vol = "L_" + filesys["volume_name"] + \
                             "_" + snap_name
                self.log("info", "Expect snapshot '{0}'".format(expect_ss_vol))
                self.assertFalse(self.is_text_in_list(expect_ss_vol, out))

                self.assertFalse(self.is_text_in_list("ERROR", err))
        sshot_list = self.get_snapshots([self.ms_node])
        self.assertEqual([], sshot_list)

        # Verify that the named snapshot item is removed from /snapshots
        self.assertFalse(self.is_snapshot_item_present(
            self.ms_node, snap_name))

    @attr('all', 'non-revert', 'story111665', 'story111665_tc07')
    def test_07_p_update_ks_fss_size(self):
        '''
        @tms_id: TORF_111665_tc07
        @tms_requirements_id: TORF-111665
        @tms_title: update kickstart fileystem sizes
        @tms_description:
            This test will verify that modeled kickstart filesystems can be
            updated with new sizes greater than existing values.
        @tms_test_steps:
            @step: Remove all snapshots.
            @result: All snapshots were removed.
            @step: Update the size and snap_size of the /var, /var/log,
                  /var/www kickstart filesystems.
            @result: Successfully updated /var, /var/log, /var/www kickstart
                     filesystems.
            @step: Create/Run Plan
            @result: Successfully created and run plan.
            @step: Check that /var, /var/log, /var/www kickstart filesystems
                   were updated with the new sizes.
            @result: /var, /var/log, /var/www have the sizes updated.
        @tms_test_precondition: This test unit has a dependency on$
                                testset_story12270_418338.py which must pass$
                                before this test can be run successfully.
        @tms_execution_type: Automated
        '''
        self.remove_all_snapshots(self.ms_node)

        vg_root_url = self.get_root_volume_group_url("/ms/storage_profile")

        ks_fss = {'var': {
                       'mount_point': '/var',
                       'size': '11G',
                       'snap_size': '90',
                       'url': vg_root_url + "/file_systems/var"},
                'var_log': {
                       'mount_point': '/var/log',
                       'size': '21G',
                       'snap_size': '90',
                       'url': vg_root_url + "/file_systems/var_log"},
                'var_www': {
                    'mount_point': '/var/www',
                    'size': '141G',
                    'snap_size': '90',
                    'url': vg_root_url + "/file_systems/var_www"}}

        self.log("info", "Update the size and snap_size of the "
                "/var, /var/log, /var/www kickstart filesystems.")
        for ks_fs_props in ks_fss.itervalues():
            props = \
                "size='{0}' snap_size='{1}'".format(ks_fs_props['size'],
                                                    ks_fs_props['snap_size'])
            self.execute_cli_update_cmd(self.ms_node, ks_fs_props['url'],
                                        props)

        self.log("info", "Create/Run Plan.")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                                plan_timeout_mins=30)

        self.log("info", "Check that /var, /var/log, /var/www "
                 "kickstart fileystems were updated with the new sizes")
        for ks_fs_props in ks_fss.itervalues():
            self.verify(self.ms_node, ks_fs_props['url'],
                                      ks_fs_props['size'],
                                      ks_fs_props["mount_point"])
