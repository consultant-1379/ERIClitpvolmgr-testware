'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     April 2015
@author:    Philip Daly
@summary:   Integration
            Agile: LITPCDS-6425, TORF-113332
'''

from litp_generic_test import GenericTest, attr
import random


class Story6425(GenericTest):
    """
    LITPCDS-6425
    As a LITP user I want to be able to set snap_size percentage on
    VxVM volumes to any number between 0 and 100 so I have full control
    over the size of my VxVM snapshots

    TORF-113332
    As a LITP user, I want to be able to specify a snap_size for my
    backup snapshots independent of my snap_size for my deployment snapshots so
    that I can take snapshots of varying size
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
        super(Story6425, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.test_nodes = self.get_managed_node_filenames()

        self.node_urls = self.find(self.ms_node, "/deployments", "node")
        self.node_urls.sort()

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
        super(Story6425, self).tearDown()

    def snap_size_update(self, fss):
        """
        Description:
            Function to update the snap_size property below the provided
            file system objects to be the opposite of what it was. Snapshots
            must exist at this point as this function is a validation
            test to ensure that validation errors are returned at create_plan
            stage.
        Args:
            fss (list): A list of file-system urls.
        Returns:
            cleanup_list. list. A list of the fs urls to be restored to their
                                previous value following the test.
        """
        cleanup_list = []
        for fsystem in fss:
            value = fsystem['snap_size']
            cleanup_list.append(fsystem['path'])
            if value != "0":
                opposite_value = "0"
            else:
                min_size = self.get_min_vxvm_snap_size(
                        self.ms_node, fsystem['path'])
                opposite_value = random.randint(min_size, 100)
            self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                           props='snap_size={0}'.format(opposite_value))

        return cleanup_list

    def snap_external_update(self, fss):
        """
        Description:
            Function to update the snap_external property below the provided
            file system objects to be the opposite of what it was. Snapshots
            must exist at this point as this function is a validation
            test to ensure that validation errors are returned at create_plan
            stage.
        Args:
            fss (dict): A dictionary of all the file systems below
                                  each node.
        """
        opposite_values = {'true': 'false', 'false': 'true'}
        cleanup_list = []
        for fsystem in fss:
            value = fsystem['snap_external']
            cleanup_list.append(fsystem['path'])
            opposite_value = opposite_values[value]
            self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                        props='snap_external={0}'.format(opposite_value))

        return cleanup_list

    def create_verify_remove_verify(self, fss, snap_name=''):
        """
        Function to issue the create_snapshot and remove_snapshot commands. It
        also verifies that the expected snapshots are created/removed. Certain
        validation tests are also executed within this function.

        Args:
            fss (list): List of file systems.
            snap_name (str): The user defined snapshot name that is included
                             in to the snapshot images.
        """
        # 6.1 CREATE SNAPSHOT
        args = ''
        if snap_name != '':
            args = '--name {0}'.format(snap_name)

        self.execute_and_wait_createsnapshot(self.ms_node, args=args)

        # VERIFY CACHE HERE
        self.verify_fs_cache(fss, snap_name)
        # UPDATE SNAP_EXTERNAL AND SNAP_SIZE VALUE
        # REMOVE SHOULD TAKE PLACE ON ALL OF THE SNAPSHOTS CREATED
        # THUS PROVING LITPCDS-9903
        snap_size_cleanup_list = []
        snap_external_cleanup_list = []
        snap_size_cleanup_list = \
        self.snap_size_update(fss)
        snap_external_cleanup_list = \
        self.snap_external_update(fss)

        self.execute_and_wait_removesnapshot(self.ms_node, args=args)

        # VERIFY CACHES REMOVED HERE
        self.verify_fs_cache(fss, snap_name, expective_positive=False)
        # REVERT THE UPDATES MADE TO THE SNAP_SIZE AND SNAP_EXTERNAL
        for url in snap_size_cleanup_list:
            self.execute_cli_update_cmd(self.ms_node, url,
                                        props='snap_size',
                                        action_del=True)
        for url in snap_external_cleanup_list:
            self.execute_cli_update_cmd(self.ms_node, url,
                                        props='snap_external',
                                        action_del=True)

    def verify_fs_cache(self, fss, snap_name, expective_positive=True):
        """
        Function to verify the cache sizes created.
        Args:
            fss (list): List of file systems.
            snap_name (str): Name assigned to the snapshot.
            expective_positive (bool): Flag to check existence of cache.
        """
        console = self.get_vxprint_console_output()

        for fsystem in fss:
            self.assertTrue(
                    self.is_text_in_list(fsystem['vg_item_id'], console))
            fs_size_in_mb = self.sto.convert_size_to_megabytes(fsystem['size'])
            fs_plex_size = self.get_plex_size(fs_size_in_mb)
            fs_cache_name = "LO{0}_{1}".format(
                    fsystem['volume_name'], snap_name)
            fs_snap_size = self.get_snap_plex_size(fs_size_in_mb,
                    fsystem, snap_name)

            # VERIFY THAT THE CORRECT NUMBER OF PLEXES EXIST FOR THE FS
            fs_index = \
            [i for i, s in enumerate(console) if
                " {0}".format(fsystem['volume_name']) in s][0]
            self.assertTrue(fs_plex_size in console[fs_index])
            # VERIFY THAT THE CORRECT NUMBER OF PLEXES EXIST FOR THE CACHE
            cache_index = [i for i, s in enumerate(console)
                    if "{0}".format(fs_cache_name) in s]
            if expective_positive == True:
                # use backup snap size for named snapshots if it is set
                self.assertTrue(fs_snap_size in console[cache_index[0]])
            else:
                self.assertEqual([], cache_index)

    def get_vxprint_console_output(self):
        """
        Function to get the console output of vxprint.
        Returns:
            str. The vxprint -vt console output.
        """
        # 6 FROM TEST BELOW
        console_output = []
        for node in self.test_nodes:
            stdout, _, _ = \
            self.run_command(node,
                             '/usr/sbin/vxprint -vt', su_root=True)
            console_output.extend(stdout)
        return console_output

    @staticmethod
    def get_plex_size(fs_size):
        """
        Function to get the plex size of the file system.
        Args:
            fs_size (str): The size of the file system.
        Returns:
            str. The plex size of the file system.
        """
        return str(int(float(fs_size) * 1024 * 2))

    @staticmethod
    def get_snap_plex_size(fs_size, fsystem, snap_name):
        """
        Function to return the size in plexes of the cache size.
        Args:
            fs_size (str): the size in mb of the file system.
            fs (str): file system dict
            snap_name: name of the snap shot
        Returns:
            str. The plex size of the cache.
        """
        # named backup snapshot with backup_snap_size set
        if fsystem.get('backup_snap_size') and snap_name:
            snap_size = fsystem['backup_snap_size']
        else:
            snap_size = fsystem['snap_size']
        return str(int(float(fs_size) * float(snap_size) / 100) * 1024 * 2)

    def set_snap_size(self, fss, prop, value=None):
        """
        sets the snaphot size/backup snapshot size to its minimum supported
        value
        """
        for fsystem in fss:
            if not value:
                value = self.get_min_vxvm_snap_size(self.ms_node,
                        fsystem['path'])
            self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                    props='{0}={1}'.format(prop, value))
            fsystem[prop] = value
        return fss

    @attr('all', 'revert', 'story6425', 'story6425_tc04', 'story113332',
          'story113332_tc04', 'kgb-physical')
    def test_04_p_filesys_snap_size_cache_size_create_remove(self):
        '''
        @tms_id: litpcds_6425_tc04
        @tms_requirements_id: LITPCDS-6425, TORF-113332
        @tms_title: Verify vxfs snap size setting
        @tms_description:
            To ensure that the value set for the snap_size property on the
            file system object is reflected on the size of the cache created
            for the snapshot.
        @tms_test_steps:
            @step: Remove snapshots
            @result: Snapshots are removed
            @step: Set the snap_size of the vxfs file system to a valid random
                value
            @result: snap_size value is updated
            @step: create deployment snapshot
            @result: vxfs snapshot size is according to model setting
            @step: create named snapshot
            @result: vxfs snapshot size is according to model setting
            @step: set the snap_size value to the minimum supported value
            @result:  snap_size value is updated
            @step: create deployment snapshot
            @result: vxfs snapshot size is according to model setting
            @step: create named snapshot
            @result: vxfs snapshot size is according to model setting
            @step: set the snap_size value to the maximum supported value
            @result:  snap_size value is updated
            @step: create deployment snapshot
            @result: vxfs snapshot size is according to model setting
            @step: create named snapshot
            @result: vxfs snapshot size is according to model setting
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        '''
        self.log('info', 'Remove snapshots')
        snap_name = "6425"
        snap_size = 90
        self.remove_all_snapshots(self.ms_node)

        fss = self.get_all_volumes(self.ms_node)

        for fsystem in fss:
            self.backup_path_props(self.ms_node, fsystem['path'])

        self.assertNotEqual([], fss)

        self.log('info',
           'Set the snap_size of the vxfs file system to a valid random value')
        for fsystem in fss:
            min_value = self.get_min_vxvm_snap_size(self.ms_node,
                    fsystem['path'])
            self.execute_cli_update_cmd(self.ms_node, fsystem['path'],
                                    props='{0}={1}'.format('backup_snap_size',
                                        min_value))
            fsystem['backup_snap_size'] = snap_size

        # VERIFY THAT THE CORRECT SIZE SNAPSHOTS HAVE BEEN CREATED
        self.log('info', 'create deployment snapshot')
        self.create_verify_remove_verify(fss)

        fss = self.get_all_volumes(self.ms_node)
        self.log('info', 'create named snapshot')
        self.create_verify_remove_verify(fss, snap_name)
        # UPDATE THE FILESYSTEMS TO THEIR MINIMUM ALLOWED
        # SNAP_SIZE VALUE deployment snapshots
        fss = self.set_snap_size(fss, "snap_size")

        # VERIFY THAT THE CORRECT SIZE SNAPSHOTS HAVE BEEN CREATED
        self.log('info', 'create deployment snapshot')
        self.create_verify_remove_verify(fss)

        # UPDATE THE FILESYSTEMS TO THEIR MINIMUM ALLOWED
        # SNAP_SIZE VALUE named snapshots
        fss = self.set_snap_size(fss, "backup_snap_size")
        self.log('info', 'create named snapshot')
        self.create_verify_remove_verify(fss, snap_name)

        fss = self.set_snap_size(fss, "backup_snap_size", value=100)
        self.log('info', 'create named snapshot')
        self.create_verify_remove_verify(fss, snap_name)

        # UPDATE THE FILESYSTEMS TO THEIR MAXIMUM ALLOWED
        # SNAP_SIZE VALUE
        fss = self.set_snap_size(fss, "snap_size", value=100)

        # VERIFY THAT THE CORRECT SIZE SNAPSHOTS HAVE BEEN CREATED
        self.log('info', 'create deployment snapshot')
        self.create_verify_remove_verify(fss)
