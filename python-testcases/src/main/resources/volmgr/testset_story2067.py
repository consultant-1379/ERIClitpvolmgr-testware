'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     October  2014
@author:    Kieran Duggan
@summary:   Integration
            Agile: STORY-2067
'''

from litp_generic_test import GenericTest, attr
from litp_cli_utils import CLIUtils
from storage_utils import StorageUtils
import test_constants
from math import fabs


class Story2067(GenericTest):
    """
    As a LITP User I want to increase the size of a
    LVM Logical Volume  and the filesystem that lives
    on it, so that I can allocate more space for my application
    """

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
        super(Story2067, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.test_nodes = self.get_managed_node_filenames()
        self.test_nodes.sort()
        self.cli = CLIUtils()
        self.storage_utils = StorageUtils()

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
        super(Story2067, self).tearDown()

    @staticmethod
    def increase_size_gb(size):
        """
        If size is in G returns size+1
        else returns the given size
        """
        if "G" in size:
            value, _ = size.split("G")
            value = int(value)
            value += 1
            size = str(value) + "G"

        return size

    @staticmethod
    def increase_size_mb(size):
        """
        If size is in G returns size+1
        else returns the given size
        """
        if "M" in size:
            value, _ = size.split("M")
            value = int(value)
            # Must be multiple of 4MB and at least 0.01GB
            value += 12
            size = str(value) + "M"

        return size

    @staticmethod
    def convert_to_gb(size):
        """
        Converts given size to G
        """
        if "M" in size:
            value, _ = size.split("M")
            value = int(value)
            value /= 1024
            size = str(value) + "G"

        return size

    def force_initial_size_unit(self, fs_path, wanted_unit, current_size):
        """
        If test specification is convert from G to M or viceversa
        and current size doesnt fulfil, force a change to
        have what test needs.
        """
        if wanted_unit not in current_size:
            if 'M' in current_size:
                converted_size = self.convert_to_gb(current_size)
            else:
                converted_size = str(self.storage_utils.convert_gb_to_mb(
                        current_size)) + "M"
            self.execute_cli_update_cmd(self.ms_node, fs_path,
                                    "size={0}".format(converted_size))

            self.execute_cli_createplan_cmd(self.ms_node)
            self.execute_cli_runplan_cmd(self.ms_node)
            self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                        test_constants.PLAN_COMPLETE))

            std_out, _, _ = self.execute_cli_show_cmd(self.ms_node, fs_path)
            self.assertTrue(self.is_text_in_list("size: {0}".\
                                        format(converted_size), std_out))

            current_size = converted_size

        return current_size

    def get_file_systems(self, fs_type, node_vpath, mount_point):
        '''
        Looks for a filesystem of a specific type on a node vpath.
        Return a list of found fss of desired type and mount point
        or, if "non-root" specified, a list of all xfs fs but /
        and /swap
        '''
        file_systems = self.find(self.ms_node, node_vpath, "file-system")
        filesystems_list = []

        for fs_url in file_systems:
            props = self.get_props_from_url(self.ms_node, fs_url)

            if props["type"] != fs_type:
                continue

            if props["mount_point"] == mount_point or \
               (props["mount_point"] not in ('/', '/swap') and \
               mount_point == "non-root"):
                filesystems_list.append((fs_url, props["size"]))

        return filesystems_list

    def get_all_nodes_fss_sizes(self, target_mount='/'):
        '''
        Returns a collection of node_names and current size with
        all fss of type xfs and mounted in /
        '''
        result = {}
        nodes_urls = self.find(self.ms_node, "/deployments", "node", True)
        for node_url in nodes_urls:
            fs_url, _ = self.get_file_systems(
                'xfs', node_url, mount_point=target_mount)[0]
            size = self.get_props_from_url(\
                self.ms_node, fs_url, show_option='').get('size')
            node_name = self.get_props_from_url(\
                             self.ms_node, node_url).get('hostname')
            result[node_name] = size
        return result

    def _get_real_fs_size_gb(self, node, mount_point):
        '''
        Returns (as a float representing GB) the size of the logical volume
        which is mounted on given mount point on given node.
        '''
        # Queries what an LV (and hence, filesystem) size is on disk.
        # Used to verify that applied model sizes reflect reality.
        std_out, _, _ = self.run_command(
            node, 'df -h -P -l', su_root=True
            )
        for str_line in std_out[1:]:
            tokens = str_line.split()
            if tokens[5] == mount_point:
                std_out_2, _, _ = self.run_command(
                    node,
                    '/sbin/lvs -o size --noheadings {0}'.format(tokens[0]),
                    su_root=True
                    )
                size_str = std_out_2[0].strip()
                if size_str[-1] == 'g':
                    return float(size_str[:-1])
                elif size_str[-1] == 'm':
                    return float(size_str[:-1]) / 1024.0
                else:
                    return float(size_str[:-1]) * 1024.0
        return None

    def _get_real_fs_size_mb(self, node, mount_point):
        '''
        Returns (as an Int representing MB) the size of the logical volume
        which is mounted on given mount point on given node.
        '''
        # Queries what an LV (and hence, filesystem) size is on disk.
        # Used to verify that applied model sizes reflect reality.
        std_out, _, _ = self.run_command(
            node, 'df -h -P -l', su_root=True
            )
        for str_line in std_out[1:]:
            tokens = str_line.split()
            if tokens[5] == mount_point:
                std_out_2, _, _ = self.run_command(
                    node,
                    '/sbin/lvs -o size --noheadings {0}'.format(tokens[0]),
                    su_root=True
                    )
                size_str = std_out_2[0].strip()
                if size_str[-1] == 'm':
                    return int(size_str[:-1])
                elif size_str[-1] == 'g':
                    size_str = size_str[:-1]
                    split_str = size_str.split('.')
                    change_str = "0." + split_str[1]
                    return (int(split_str[0]) * 1024) + \
                           (int(float(change_str) * 1024))
                else:
                    size_str = size_str[:-1]
                    split_str = size_str.split('.')
                    change_str = "0." + split_str[1]
                    return (int(split_str[0]) * 1024) + \
                           (int(float(change_str) * 1024))
        return None

    def create_test_mount_point(self):
        """
        Create a mount point in /root/test under a storage profile of volume
        driver type lvm.
        """
        profile_urls = \
        self.find(self.ms_node, "/infrastructure", "storage-profile")
        # FIND A LVM PROFILE
        lvm_found = False
        lvm_fs_url = ""
        for profile_url in profile_urls:
            vol_driver = \
            self.get_props_from_url(self.ms_node, profile_url,
                                   filter_prop="volume_driver")
            if vol_driver == "lvm":
                lvm_found = True
                lvm_fs_url = profile_url
                break
        self.assertTrue(lvm_found)
        fs_url = self.find(self.ms_node, lvm_fs_url, "file-system")[0]
        split_fs_url = fs_url.split('/')
        split_fs_url.pop(-1)
        test_url = \
        "".join(["/" + x for x in split_fs_url if x != '']) + '/test'
        _, stderr, rt_code = \
        self.execute_cli_create_cmd(self.ms_node, test_url, "file-system",
                     props='type=xfs mount_point=/root/test size=1024M',
                                    add_to_cleanup=False)
        self.assertEqual([], stderr)
        self.assertEqual(0, rt_code)
        # CREATE AND RUN PLAN TO DEPLOY NEW MOUNT POINT
        self.execute_cli_createplan_cmd(self.ms_node)
        self.execute_cli_runplan_cmd(self.ms_node)
        self.assertTrue(self.wait_for_plan_state(self.ms_node,
                                    test_constants.PLAN_COMPLETE))

    @attr('all', 'non-revert', 'story2067', 'story2067_tc01')
    def test_01_p_increase_size_all_nodes_GB(self):
        """
        @tms_id: litpcds_2067_tc01
        @tms_requirements_id: LITPCDS-2067
        @tms_title: Increase root file system size on all nodes
        @tms_description:
            Increases the size of the root file system on all nodes
        @tms_test_steps:
            @step: Remove existing snapshots
            @result: Snapshots are removed
            @step: Update the size of a file-system of type xfs mounted on /
                in infrastructure
            @result: Size is updated
            @step: Create and run the plan
            @result: Plan succeeds
            @result: updated size is still in the model
            @result: all inherited vpaths have the new size
            @result: lvm reported size is similar to the value in the model
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        # Fetch actual initial LV sizes
        init_sizes = {}
        for node in self.test_nodes:
            init_sizes[node] = self._get_real_fs_size_gb(node, '/')

        self.log('info', 'Remove existing snapshots')
        self.execute_and_wait_removesnapshot(self.ms_node)

        self.log('info', 'Update the size of a file-system of type xfs'
                'mounted on / in /infrastructure')
        nodes_url = self.find(self.ms_node, "/deployments", "node", True)
        node1_vpath = nodes_url[0]
        node1_fs_url, _ = self.get_file_systems(\
                                    'xfs', node1_vpath, "/")[0]

        file_sys_infras = self.deref_inherited_path(self.ms_node, node1_fs_url)
        get_data_cmd = self.cli.get_show_data_value_cmd(file_sys_infras,
                                                    'size')

        stdout, _, _ = self.run_command(self.ms_node,
                                               get_data_cmd)
        base_fs_original_size = self.convert_to_gb(stdout[0])
        size = self.increase_size_gb(base_fs_original_size)
        self.execute_cli_update_cmd(self.ms_node, file_sys_infras,
                                        "size={0}".format(size))

        self.log('info', 'Create and run the plan')
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=600)

        #Verify updated size is still in the model
        std_out, _, _ = self.execute_cli_show_cmd(self.ms_node,
                                                  file_sys_infras)
        self.assertTrue(self.is_text_in_list("size: {0}".\
                                            format(size), std_out))

        # Check that all inherited vpaths have the new size.
        for _, new_size_str in self.get_all_nodes_fss_sizes().items():
            if '*' in new_size_str:
                new_size = new_size_str.split()[0]
                self.assertEqual(size, new_size)

        # Verify lvm reported size is similar to the value in the model
        for node, init_size in init_sizes.items():
            final_size = self._get_real_fs_size_gb(node, '/')
            delta_size = final_size - init_size
            self.assertEqual(delta_size, 1.0)

    @attr('all', 'non-revert', 'story2067', 'story2067_tc05')
    def test_05_p_increase_size_one_node_only(self):
        """
        @tms_id: litpcds_2067_tc05
        @tms_requirements_id: LITPCDS-2067
        @tms_title: Increase root file system size on one node
        @tms_description:
            Increases the size of the root file system on one node
        @tms_test_steps:
            @step: Update the size of a file-system of type xfs mounted on /
                for one node
            @result: Size is updated
            @step: Remove existing snapshots
            @result: Snapshots are removed
            @step: Create and run the plan
            @result: Plan succeeds
            @result: source item size is not changed
            @result: only node file system has changed
            @result: lvm reported size is similar to the value in the model
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        # Fetch initial LV sizes
        init_sizes = {}
        for node in self.test_nodes:
            init_sizes[node] = self._get_real_fs_size_gb(node, '/')

        self.log('info', 'Update the size of a file-system of type xfs'
                'mounted on / for one node')
        nodes_url = self.find(self.ms_node, "/deployments", "node", True)
        node1_vpath = nodes_url[0]
        node1_fs_url, node1_size = self.get_file_systems(\
                                    'xfs', node1_vpath, "/")[0]

        self.log('info', 'Remove existing snapshots')
        self.execute_and_wait_removesnapshot(self.ms_node)
        initial_sizes = self.get_all_nodes_fss_sizes()

        # get infrastructure fs size
        file_sys_infras = self.deref_inherited_path(self.ms_node, node1_fs_url)

        get_data_cmd = self.cli.get_show_data_value_cmd(file_sys_infras,
                                                        'size')

        stdout, _, _ = self.run_command(self.ms_node, get_data_cmd)
        base_fs_original_size = stdout[0]

        # update node1
        node_original_size = self.force_initial_size_unit(fs_path=node1_fs_url,
                                                     wanted_unit='M',
                                                     current_size=node1_size)

        node1_modified_size = self.increase_size_mb(node_original_size)
        self.execute_cli_update_cmd(self.ms_node, node1_fs_url,
                                  "size={0}".format(node1_modified_size))

        self.log('info', 'Create and run the plan')
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=600)

        #source should not be changed
        std_out, _, _ = self.execute_cli_show_cmd(self.ms_node,
                                                  file_sys_infras)
        self.assertTrue(self.is_text_in_list("size: {0}".\
                                      format(base_fs_original_size), std_out))

        #Check that only modified node1 has changed
        for node_name, new_size in self.get_all_nodes_fss_sizes().items():
            final_size = self._get_real_fs_size_gb(node_name, '/')
            delta_size = final_size - init_sizes[node_name]
            if '*' in new_size:
                self.assertEqual(initial_sizes.get(node_name), new_size)
                self.assertEqual(delta_size, 0.0)
            else:
                self.assertEqual(node1_modified_size, new_size.split()[0])
                # Damn you, IEEE 754 approximations..
                self.assertTrue(fabs(delta_size - 0.01) < 0.00001)

    @attr('all', 'non-revert', 'story2067', 'story2067_tc06', 'physical')
    def test_06_p_increase_size_all_nodes_mount_data_GB(self):
        """
        @tms_id: litpcds_2067_tc06
        @tms_requirements_id: LITPCDS-2067
        @tms_title: Increase data file system size on all nodes
        @tms_description:
            Increases the size of a non root file system on all nodes
        @tms_test_steps:
            @step: Find or create a non root file system
            @result: non root file system has been identified/created
            @step: Remove existing snapshots
            @result: Snapshots are removed
            @step: Update the size of the found non root file
            system in infrastructure
            @result: Size is updated
            @step: Create and run the plan
            @result: Plan succeeds
            @result: updated size is still in the model
            @result: all inherited vpaths have the new size
            @result: lvm reported size is similar to the value in the model
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        self.log('info', 'Find or create a non root file system')
        nodes_url = self.find(self.ms_node, "/deployments", "node", True)
        node1_vpath = nodes_url[0]

        self.log('info', 'Remove existing snapshots')
        self.execute_and_wait_removesnapshot(self.ms_node)
        # Find non-root fs to extend. Assume one by the same name
        # exists on both MNs.
        mount_found = False
        file_systems = self.find(self.ms_node, node1_vpath, "file-system")
        for fs_url in file_systems:
            fs_props = self.get_props_from_url(self.ms_node, fs_url)
            if fs_props["type"] == 'xfs' and \
                fs_props["mount_point"] not in ('/', '/swap'):
                target_mountpoint = fs_props["mount_point"]
                mount_found = True
                break
        if mount_found == False:
            # CREATE A NEW MOUNT POINT TO CONTINUE TEST
            self.create_test_mount_point()
            file_systems = self.find(self.ms_node, node1_vpath, "file-system")
            for fs_url in file_systems:
                fs_props = self.get_props_from_url(self.ms_node, fs_url)
                if fs_props["type"] == 'xfs' and \
                    fs_props["mount_point"] not in ('/', '/swap'):
                    target_mountpoint = fs_props["mount_point"]
                    mount_found = True
                    break

        init_sizes = {}
        for node in self.test_nodes:
            init_sizes[node] = self._get_real_fs_size_gb(
            node,
            target_mountpoint
            )

        self.log('info', 'Update the size of a file-system of the found non '
                'root file system in infrastructure')
        fs_urls = self.get_file_systems('xfs', node1_vpath, target_mountpoint)
        self.assertTrue(len(fs_urls) > 0)
        node1_fs_url, _ = fs_urls[0]
        file_sys_infras = self.deref_inherited_path(self.ms_node, node1_fs_url)
        get_data_cmd = self.cli.get_show_data_value_cmd(file_sys_infras,
                                                        'size')

        stdout, _, _ = self.run_command(self.ms_node, get_data_cmd)

        base_fs_original_size = \
            self.force_initial_size_unit(fs_path=file_sys_infras,
                                         wanted_unit='G',
                                         current_size=stdout[0])

        size_gb = self.convert_to_gb(base_fs_original_size)
        size = self.increase_size_gb(size_gb)

        self.execute_cli_update_cmd(self.ms_node, file_sys_infras,
                                "size={0}".format(size))

        self.log('info', 'Create and run the plan')
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=600)

        std_out, _, _ = self.execute_cli_show_cmd(self.ms_node,
                                                    file_sys_infras)
        # verfy updated size is still in the model
        self.assertTrue(self.is_text_in_list("size: {0}".\
                                    format(size), std_out))

        #Check all inherited vpaths have the new size
        fs_sizes = self.get_all_nodes_fss_sizes(target_mount=target_mountpoint)
        for _, new_size in fs_sizes.items():
            if '*' in new_size:
                self.assertEqual(size, new_size.split()[0])
        for node, init_size in init_sizes.items():
            # verify lvm reported size is similar to the value in the model
            final_size = self._get_real_fs_size_gb(node, target_mountpoint)
            delta_size = final_size - init_size
            self.assertEqual(delta_size, 1.0)

    @attr('all', 'non-revert', 'physical', 'story2067', 'story2067_tc07')
    def test_07_p_increase_size_one_node_only_mounted_data(self):
        """
        @tms_id: litpcds_2067_tc07
        @tms_requirements_id: LITPCDS-2067
        @tms_title: Increase non root file system size on one node
        @tms_description:
            Increases the size of the non root file system on one node
        @tms_test_steps:
            @step: Find or create a non root file system
            @result: non root file system has been identified/created
            @step: Remove exist snapshots
            @result: Snapshots are removed
            @step: Update the size of the found non root filesystem for one
            node
            @result: Size is updated
            @step: Create and run the plan
            @result: Plan succeeds
            @result: source item size is not changed
            @result: only node file system has changed
            @result: lvm reported size is similar to the value in the model
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """

        self.log('info', 'Find or create a non root file system')
        nodes_url = self.find(self.ms_node, "/deployments", "node", True)
        node1_vpath = nodes_url[0]

        # Find non-root xfs mountpoint.
        mount_found = False
        file_systems = self.find(self.ms_node, node1_vpath, "file-system")
        for fs_url in file_systems:
            fs_props = self.get_props_from_url(self.ms_node, fs_url)
            if fs_props["type"] == 'xfs' and \
                fs_props["mount_point"] not in ('/', '/swap'):
                target_mountpoint = fs_props["mount_point"]
                mount_found = True
                break
        if mount_found == False:
            # CREATE A NEW MOUNT POINT TO CONTINUE TEST
            self.create_test_mount_point()
            file_systems = self.find(self.ms_node, node1_vpath, "file-system")
            for fs_url in file_systems:
                fs_props = self.get_props_from_url(self.ms_node, fs_url)
                if fs_props["type"] == 'xfs' and \
                    fs_props["mount_point"] not in ('/', '/swap'):
                    target_mountpoint = fs_props["mount_point"]
                    mount_found = True
                    break
        fs_urls = self.get_file_systems(
            'xfs', node1_vpath, target_mountpoint
            )
        self.assertTrue(len(fs_urls) > 0)
        node1_fs_url, node1_size = fs_urls[0]

        self.log('info', 'remove existing snapshots')
        self.execute_and_wait_removesnapshot(self.ms_node)
        initial_sizes = self.get_all_nodes_fss_sizes(
            target_mount=target_mountpoint
            )
        init_sizes = {}
        for node in self.test_nodes:
            init_sizes[node] = self._get_real_fs_size_mb(
                node,
                target_mountpoint
                )

        self.log('info', 'Update the size of the found non root filesystem for'
                ' one node')
        # get infrastructure fs size
        file_sys_infras = self.deref_inherited_path(self.ms_node, node1_fs_url)

        get_data_cmd = self.cli.get_show_data_value_cmd(file_sys_infras,
                                                        'size')

        stdout, _, _ = self.run_command(self.ms_node, get_data_cmd)
        base_fs_original_size = stdout[0]

        # update node1
        node_original_size = self.force_initial_size_unit(fs_path=node1_fs_url,
                                                     wanted_unit='M',
                                                     current_size=node1_size)

        node1_modified_size = self.increase_size_mb(node_original_size)
        self.execute_cli_update_cmd(self.ms_node, node1_fs_url,
                                  "size={0}".format(node1_modified_size))

        self.log('info', 'Create and run the plan')
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=600)

        #verify source item size is not changed
        std_out, _, _ = self.execute_cli_show_cmd(self.ms_node,
                                                  file_sys_infras)
        self.assertTrue(self.is_text_in_list("size: {0}".\
                                      format(base_fs_original_size), std_out))

        #Check that only modified node1 has changed
        for node_name, new_size in self.get_all_nodes_fss_sizes(
            target_mount=target_mountpoint).items():
            final_size = self._get_real_fs_size_mb(
                node_name,
                target_mountpoint
                )

            delta_size = final_size - init_sizes[node_name]
            if '*' in new_size:
                self.assertEqual(initial_sizes.get(node_name), new_size)
                #check lvm reported size is similar to the value in the model
                self.assertEqual(delta_size, 0.0)
            else:
                self.assertEqual(node1_modified_size, new_size.split()[0])
                self.assertTrue(init_sizes[node_name] < final_size)
