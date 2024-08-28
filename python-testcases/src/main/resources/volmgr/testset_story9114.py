"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     July 2015
@author:    Brian Duffy, Conor Broderick
@summary:   Integration tests
            Agile: LITPCDS-9114

            NOTE:
            These test cases require physical HW. However, as they resize
            the filesystem, they cannot be added to PKGB/CDB until a story
            in introduced to decrease filesystem size. Must be tagged as
            manual for the moment.
"""

from __future__ import division

import re
import test_constants

from litp_generic_test import GenericTest, attr


class Story9114(GenericTest):
    """
    As a LITP User I want to increase the size of a VxVM volume and the
    filesystem that lives on it, so that I can allocate more space for my
    application
    """

    exponents = {'P': 5, 'T': 4, 'G': 3, 'M': 2, 'K': 1, '': 0}

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
        super(Story9114, self).setUp()

        # 2. Set up variables used in the test
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()

        # increment size
        self.increase_size = '10M'

        # Expected error
        self.expected_snapshot_error = \
        "Changing the \"size\" property of an \"ext4\" or \"vxfs\" " \
        "file system while a snapshot exists is not supported"

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
        # 1. Call super class setup
        super(Story9114, self).tearDown()

    def _ms_has_snapshots(self):
        """
        Description:
            Check if there are any snapshots on any of the test nodes in the
            cluster.

        Returns:
            A boolean indicating if the snapshots are present in the model.
        """
        snapshot_urls = self.find(self.ms_node, "/snapshots",
                                  "snapshot-base", assert_not_empty=False)
        return snapshot_urls != []

    def _remove_snapshots(self):
        """
        Description:
            Removes any snapshots from the plan that exist in the model.
        """
        if self._ms_has_snapshots():
            self.execute_cli_removesnapshot_cmd(self.ms_node)
            self.wait_for_plan_state(self.ms_node,
                                     test_constants.PLAN_COMPLETE)

    def _get_vxvm_file_systems_urls(self):
        """
        Description:
            Examines all storage profiles on the management server and returns
            all file-systems that are managed by VXVM.

        Returns:
            A list of URLs for each VXVM file-system
        """
        storage_profiles = \
            self.get_storage_profile_paths(self.ms_node, 'vxvm',
                                           "/deployments")

        all_vxfs = []

        for storage_profile in storage_profiles:
            volume_groups = self.find(self.ms_node, storage_profile,
                                      "volume-group")
            for volume_group in volume_groups:
                all_vxfs.extend(
                    self.find(self.ms_node, volume_group, "file-system")
                )

        return all_vxfs

    def _get_vxfs_size(self, vxfs):
        """
        Description:
            Given a VXFS URL finds the size of the file-system.

        Args:
            vxfs (list): URL for a VXVM managed file-system.

        Returns:
            string representation of the size of the file-system.
        """
        vxfs_props = self.get_props_from_url(self.ms_node, vxfs)
        return vxfs_props["size"]

    @staticmethod
    def _convert_to_bytes(str_rep, unit):
        """
        Description:
            Convert a string representation of a file-system size to bytes.

        Args:
            str_rep (str): numeric part of size represented as a string
            unit (str): size unit

        Returns:
            Integer value representing bytes
        """
        exp = Story9114.exponents[unit.upper()]
        return int(float(str_rep) * (1024 ** exp))

    @staticmethod
    def _convert_bytes_to_unit(byte_rep, unit):
        """
        Description:
            Converts a value in bytes to a specific unit size.

        Args:
            byte_rep (byte): the byte representation as an integer.
            unit (str): the unit as a string.

        Returns:
            Float value of byte scaled to the specified unit.
        """
        exp = Story9114.exponents[unit.upper()]
        return float(byte_rep) / (1024.0 ** exp)

    @staticmethod
    def _prefer_smaller_units(value_unit, delta_unit):
        """
        Description:
            When increasing the size of a value by an amount, the units should
            be consistent. For LITP we should prefer smaller units as all size
            values should be represented as integers.

            Therefore, always prefer smaller units to larger ones when
            doing a sum.

        Args:
            value_unit (str): the unit of the value we wish to change
            delta_unit (str): the unit of the difference ie the delta.

        Returns:
            string value representing the required unit value.
        """
        if Story9114.exponents[value_unit] == \
           Story9114.exponents[delta_unit]:
            return value_unit
        elif Story9114.exponents[value_unit] < \
             Story9114.exponents[delta_unit]:
            return value_unit
        elif Story9114.exponents[value_unit] > \
             Story9114.exponents[delta_unit]:
            return delta_unit
        else:
            return None

    @staticmethod
    def _increase(volume_size, by_amount):
        """
        Description:
            Given an size with arbitrary units increase it by an amount also
            with arbitrary units.

        Args:
            volume_size (str): string representation of the volume size.
            by_amount (str): string representation of the increase amount.

        Returns:
            string representation of the augmented value.
        """
        pattern = r'^(\d+\.?\d*)([KMGkmg]?)$'
        pattern_matches = re.match(pattern, volume_size)
        str_x, unit_x = pattern_matches.groups()
        pattern_matches = re.match(pattern, by_amount)
        str_d, unit_d = pattern_matches.groups()
        bytes_x = Story9114._convert_to_bytes(str_x, unit_x)
        bytes_d = Story9114._convert_to_bytes(str_d, unit_d)
        increased_bytes = bytes_x + bytes_d
        unit = Story9114._prefer_smaller_units(unit_x, unit_d)
        new_size = Story9114._convert_bytes_to_unit(increased_bytes, unit)
        # can only be integer size in LITP
        return str(int(new_size)) + unit

    @staticmethod
    def _convert_string_size(v_size):
        """
        Description:
            Convert a string representation of a size value to a numeric part
            and the unit part of the representation using regular expressions.

        Args:
            v_size (str): string representation of size.

        Returns:
            - Float value representing the numeric part of the size.
            - String value representing the unit part of the size.
        """
        pattern = r'^(\d+\.?\d*)([KMGkmg]?)$'
        pattern_matches = re.match(pattern, v_size)
        str_size, unit = pattern_matches.groups()
        return float(str_size), unit.upper()

    @staticmethod
    def _sizes_are_equal(size_x, size_y):
        """
        Description:
            Compare two size values and determine if they are equal. The input
            is take as sting literal with or without units. These are converted
            to bytes to do the comparison.

        Args:
            size_x (str): a string representation of a size value.
            size_y (str): a string representation of a size value.

        Returns:
            boolean indicating equality.
        """
        num_x, str_x = Story9114._convert_string_size(size_x)
        num_y, str_y = Story9114._convert_string_size(size_y)
        bytes_x = Story9114._convert_to_bytes(num_x, str_x)
        bytes_y = Story9114._convert_to_bytes(num_y, str_y)
        return bytes_x == bytes_y

    @staticmethod
    def _get_name_from_url(resource, url):
        """
        Description:
            Get a specific resource name from a given URL.

        Args:
            resource (str): the string resource to look for in the URL.
            url (str): the URL to search.

        Returns:
            string representation of the name of the resource.
        """
        parts = url.split('/')
        if resource in parts:
            return parts[parts.index(resource) + 1]
        return ''

    def _verify_vxfs_size(self, url, new_size):
        """
        Description:
            Verify for a given file-system URL that the size has been updated
            to the specified new size. This requires that the set of managed
            nodes sharing the file-system are iterated to find the current
            node that if active for that file-system and has it mounted.

            The size is verified via the output of a vxprint command run on
            the managed node.

        Args:
            url (str): The string URL of the file-system to check.
            new_size (str): The string representation of the new size.

        Returns:
            A boolean indicating if the size update has been successful.
        """
        vg_name = Story9114._get_name_from_url('volume_groups', url)
        fs_name = Story9114._get_name_from_url('file_systems', url)
        _, unit = Story9114._convert_string_size(new_size)
        cmd = ("/opt/VRTS/bin/vxprint -g %s -h -u%s %s "
               "| awk -F ' ' '{ print $2 \" \" $5 }' "
               "| grep '%s ' | awk -F ' ' '{ print $2 }'") % (vg_name, unit,
                                                              fs_name, fs_name)
        # find which node is currently active, run vxprint to check
        for node in self.mn_nodes:
            output, _, _ = self.run_command(node, cmd)
            # if there is output this is the active node
            if output:
                vxfs_size = output[0]
                return Story9114._sizes_are_equal(new_size, vxfs_size)
        return False

    def _gather_cluster_shared_file_systems(self):
        """
        Description:
            For every cluster in the LITP model find all of the shared
            file-systems associated with them.

        Returns:
            A dictionary of clusters indexed by name that has a dictionary of
            shared file-systems for each cluster, also indexed by the names
            of the file-systems.
        """
        storage_profile_urls = self.find(
            self.ms_node, "/deployments", "storage-profile"
        )

        # filter out lvm storage profiles, these will have "nodes" in the url
        vxvm_urls = [url for url in storage_profile_urls
                            if not "/nodes/" in url]
        clusters = {}

        # iterate the vcs clusters and find the
        for url in vxvm_urls:

            c_name = Story9114._get_name_from_url('clusters', url)
            deref = self.deref_inherited_path(self.ms_node, url)
            cluster_file_systems = {}
            shared_file_systems = self.find(self.ms_node, deref, 'file-system')

            for file_system in shared_file_systems:
                fs_name = Story9114._get_name_from_url('file_systems',
                                                       file_system)
                ref_path = re.sub(deref, url, file_system)
                cluster_file_systems[fs_name] = (file_system, ref_path)

            clusters[c_name] = cluster_file_systems

        return clusters

    def _find_a_file_system_is_mounted_infra(self, file_systems):
        """
        Description:
            Given a set of file-systems for a storage profile in
            infrastructure, find the first mounted one.

        Args:
            file_systems (list): list of files-system URLs on infrastructure.

        Returns:
            - The URL of the storage profile in /infrastructure
        """
        for url in file_systems:
            for node in self.mn_nodes:
                fs_name = Story9114._get_name_from_url('file_systems', url)
                if self.is_filesystem_mounted(node, fs_name):
                    return url
        return None

    def _find_a_file_system_mounted_on_cluster(self, file_systems):
        """
        Description:
            Given a set of file-systems for a storage profile on a cluster,
            find the first mounted one.

        Args:
            file_systems (list): a list of files-system URLs on a cluster.

        Returns:
            - The URL of the storage profile in /infrastructure
            - The URL of the storage profile inherited to /cluster
        """
        for file_system in file_systems.keys():
            for node in self.mn_nodes:
                if self.is_filesystem_mounted(node, file_system):
                    return file_systems[file_system]
        return None, None

    @attr('manual-test')
    def test_01_p_increase_fs_size_no_snap_infra(self):
        """
        @tms_id: litpcds_9114_tc01
        @tms_requirements_id: LITPCDS-9114
        @tms_title: Increase vxfs file system size on infrastructure item
        @tms_description:
            This test verifies that the vxfs file system size can be increased
            if the size is changed on the infrastructure item.
        @tms_test_steps:
            @step: Remove all snapshots
            @result: Snapshots are removed
            @step: Increase the size of the infrastructure vxfs item
            @result: Size is increased
            @step: create/run plan
            @result: Plan is successful
            @result: Verify vxfs disk size has been updated to the new size
            @result: Verify model disk size has been updated to the new size
        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', "Remove all snapshots")
        self._remove_snapshots()

        self.log('info', "Increase the size of the infrastructure vxfs item")

        vx_file_systems = self._get_vxvm_file_systems_urls()

        self.assertTrue(
            len(vx_file_systems) > 0,
            'there should be at least one vxfs on the lun'
        )

        vxfs = self._find_a_file_system_is_mounted_infra(vx_file_systems)

        old_size = self._get_vxfs_size(vxfs)
        new_size = self._increase(old_size, self.increase_size)

        self.execute_cli_update_cmd(
            self.ms_node, vxfs,
            "size={0}".format(new_size)
        )

        self.log('info', "create/run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=600)

        # Verify vxfs disk size has been updated to the new size
        self.assertTrue(
                self._verify_vxfs_size(vxfs, new_size),
                "disk size not the same as model"
            )

        # Verify model disk size has been updated to the new size
        new_vxfs_size = self._get_vxfs_size(vxfs)

        self.assertEqual(new_vxfs_size, new_size)

    @attr('manual-test')
    def test_03_p_increase_fs_size_no_snap_clustr(self):
        """
        @tms_id: litpcds_9114_tc03
        @tms_requirements_id: LITPCDS-9114
        @tms_title: Increase vxfs file system size on cluster item
        @tms_description:
            This test verifies that the vsfs file system size can be increased
            if the size is changed on the cluster item.
        @tms_test_steps:
            @step: Remove all snapshots
            @result: Snapshots are removed
            @step: Increase the size of the cluster vxfs item
            @result: Size is increased
            @step: create/run plan
            @result: Plan is successful
            @result: infrastructure item size has not changed
            @result: Verify vxfs disk size has been updated to the new size
            @result: Verify model disk size has been updated to the new size

        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', "Remove all snapshots")
        self._remove_snapshots()

        self.log('info', "Increase the size of the cluster vxfs item")
        clusters_with_shared_volumes = \
            self._gather_cluster_shared_file_systems()

        vx_file_systems = clusters_with_shared_volumes['c1']

        self.assertTrue(
            len(vx_file_systems) > 0,
            'there should be at least one vxfs on the lun'
        )

        infra_fs, cluster_fs = \
            self._find_a_file_system_mounted_on_cluster(vx_file_systems)

        old_infra_size = self._get_vxfs_size(infra_fs)
        old_cluster_size = self._get_vxfs_size(cluster_fs)
        new_cluster_size = self._increase(old_cluster_size, self.increase_size)

        self.execute_cli_update_cmd(self.ms_node, cluster_fs,
                                    "size={0}".format(new_cluster_size))

        self.log('info', "create/run plan")
        self.run_and_check_plan(self.ms_node, test_constants.PLAN_COMPLETE,
                plan_timeout_mins=600)

        self.wait_for_plan_state(self.ms_node,
                                 test_constants.PLAN_COMPLETE)

        infra_size = self._get_vxfs_size(infra_fs)
        cluster_size = self._get_vxfs_size(cluster_fs)

        # Verify infrastructure size has not changed
        self.assertEqual(
            old_infra_size, infra_size,
            "FS on /infrastructure %s != %s should not change %s" % \
            (old_infra_size, infra_size, infra_fs)
        )

        # Verify model disk size has been updated to the new size
        self.assertEqual(
            new_cluster_size, cluster_size,
            "FS on /cluster %s != %s found in the model for path %s" % \
            (new_cluster_size, cluster_size, cluster_fs)
        )

        # Verify vxfs disk size has been updated to the new size

        self.assertTrue(
            self._verify_vxfs_size(cluster_fs, new_cluster_size),
            "verify the the size of the volume has"
            "changed in vcs using vxprint"
        )
