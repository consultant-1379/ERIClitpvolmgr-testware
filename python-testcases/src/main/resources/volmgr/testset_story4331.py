'''
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     August 2015
@author:    Philip Daly
@summary:   Integration
            Agile: STORY-4331
'''

from litp_generic_test import GenericTest, attr
import test_constants
import math
import re


class Story4331(GenericTest):
    """
    As a LITP user, I want to support more than 1 Physical Device in a VxVM
    Volume Group (Disk Group), so that I can increase the available disk space
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
        super(Story4331, self).setUp()

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
        super(Story4331, self).tearDown()

    def verify_snap_create_if_absent(self):
        """
        Description:
            Function to verify that snapshots currently exists in the LITP
            model. If none exist it will create them.
        """
        snapshot_urls = \
        self.find(self.ms_node, "/snapshots", "snapshot-base",
                  assert_not_empty=False)
        if snapshot_urls == []:
            self.execute_cli_createsnapshot_cmd(self.ms_node)
            self.wait_for_plan_state(self.ms_node,
                                     test_constants.PLAN_COMPLETE)

    @staticmethod
    def compile_list_of_file_sys(file_sys_dict, fs_type='ext4',
                                      search_type='false'):
        """
        Description:
            Function to compile a list of the specified file system types
            residing below the file systems specified.
        Args:
            file_sys_dict (dict): Dictionary of the urls of the file systems.
            fs_type (str): The type of file system to be searched for.
            search_type (str): the dictionary key under which to retrieve all
                               found file systems of the specified type.
        Returns:
            list. A list of the file system objects.
        """
        type_url_list = []
        for node in file_sys_dict.keys():
            external_settings = file_sys_dict[node].keys()
            if search_type not in external_settings:
                continue
            file_types = file_sys_dict[node][search_type].keys()
            if fs_type in file_types:
                type_urls = file_sys_dict[node][search_type][fs_type]
                type_url_list.extend(type_urls)
        return type_url_list

    @staticmethod
    def get_fs_cache_name(fs_id, snap_name):
        """
        Function to compile the VCS cache name as it would appear in the
        vxprint command output.
        Args:
            fs_id (str): id of the fs url.
            snap_name (str): Name assigned to the snapshot.
        Return:
            str. The name of the snapshot cache as it appears in the console.
        """
        if snap_name == '':
            return "LO{0}_".format(fs_id)
        return "LO{0}_{1}".format(fs_id, snap_name)

    def verify_fs_cache(self, fs_urls, snap_name, expective_positive=True):
        """
        Function to verify the cache sizes created.
        Args:
            fs_urls (list): List of urls to the file systems.
            snap_name (str): Name assigned to the snapshot.
            expective_positive (bool): Flag to check existence of cache.
        """
        fs_dict = {}
        for fs_url in fs_urls:
            fs_id = self.strip_fs_id_from_litp_url(fs_url)
            if fs_id not in fs_dict.keys():
                fs_dict[fs_id] = {}

            fs_dict[fs_id]["size"] = \
            self.get_plex_size(self.get_fs_size(fs_url))
            fs_dict[fs_id]["snap_size"] = \
            self.get_snap_plex_size(self.get_fs_size(fs_url),
                                    self.get_fs_snap_size(fs_url))
            fs_dict[fs_id]["cache_name"] = \
            self.get_fs_cache_name(fs_id, snap_name)

        console = self.get_vxprint_console_output()

        for fs_id in fs_dict.keys():
            self.assertTrue(self.is_text_in_list(fs_id, console))
            # VERIFY THAT THE CORRECT NUMBER OF PLEXES EXIST FOR THE FS
            fs_index = \
            [i for i, s in enumerate(console) if " {0} ".format(fs_id) in s][0]
            self.assertTrue(fs_dict[fs_id]["size"] in console[fs_index])
            # VERIFY THAT THE CORRECT NUMBER OF PLEXES EXIST FOR THE CACHE
            cache_index = \
            [i for i, s in enumerate(console)
            if "{0}".format(fs_dict[fs_id]["cache_name"]) in s]
            if expective_positive == True:
                self.assertTrue(fs_dict[fs_id]["snap_size"]
                                in console[cache_index[0]])
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

    def get_fs_size(self, fs_url):
        """
        Get the size in Mb of the file system.
        Args:
            fs_url (str): The LITP url of the file system.
        Returns:
            str. The size of the file system in Mb.
        """
        fs_props = \
        self.get_props_from_url(self.ms_node, fs_url)
        fs_size = fs_props["size"]

        if "G" in fs_size:
            return self.convert_to_mb(fs_size)
        else:
            return fs_size[:-1]

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
    def get_snap_plex_size(fs_size, snap_size):
        """
        Function to return the size in plexes of the cache size.
        Args:
            fs_size (str): the size in mb of the file system.
            snap_size (str): The percent value.
        Returns:
            str. The plex size of the cache.
        """
        return str(int(float(fs_size) * float(snap_size) / 100) * 1024 * 2)

    @staticmethod
    def strip_fs_id_from_litp_url(fs_url):
        """
        Function to strip the fs id from the url supplied.
        Args:
            size (str): The size of the file system.
        Returns:
            the id of the file system stripped from the url.
        """
        return fs_url.split('/')[-1]

    def get_fs_snap_size(self, fs_url):
        """
        Get the snap size percent value of the file system.
        Args:
            fs_url (str): LITP url to the file system.
        Returns:
            str. The snap_size of the file system.
        """
        return self.get_props_from_url(self.ms_node, fs_url,
                                       filter_prop="snap_size")

    def retrieve_file_system_dict(self, vol_driver='lvm'):
        """
        Description:
            Function to retrieve a dictionary of the file systems which
            reside below each node.
        Args:
            vol_driver (str): The volume driver under test, lvm or vxvm.
        Returns:
            dict. A dictionary of all the file systems below each node.
        """
        #storage_profiles = \
        #self.get_storage_profile_paths(self.ms_node, vol_driver)
        storage_profiles = \
        self.get_storage_profile_paths(self.ms_node, vol_driver)
        # self.get_storage_profiles_from_litp_model(vol_driver)
        file_sys_dict = {}
        all_file_systems = []
        for storage_profile in storage_profiles:
            volume_groups = \
            self.get_all_vol_grps_from_storage_profile(storage_profile)
            for volume_group in volume_groups:
                all_file_systems.extend(self.get_all_file_sys_from_vol_grp(
                                        volume_group))
        for file_sys in all_file_systems:
            file_sys_dict = \
            self.compile_dict(file_sys, file_sys_dict, vol_driver)
        return file_sys_dict

    def compile_dict(self, url, fs_dict, vol_driver='lvm'):
        """
        Description:
            Function to compile a dictionary identifying the file systems
            below a node. Depending on the volume driver specified
            props shall be altered to be passed to the child function
            populate_dict.
        Args:
            url (str): A url of the node object.
            fs_dict (dict): The dictionary to be populated.
            vol_driver (str): The volume driver of the storage profile parent;
                              Could be lvm or vxvm.
        Returns:
            dict. A dictionary of all the file systems below each node.
        """
        if vol_driver == 'lvm':
            url_list = url.split('/')
            node_index = url_list.index('nodes')
            # LIST COMPREHENSION TO COMPILE NODE URL FROM CHILD URL PROVIDED
            node_url = \
            "".join(["/" + x for x in url_list[:node_index + 2] if x != ''])

            fs_dict = self.populate_dict(url, node_url, fs_dict)
        else:
            for node_url in self.node_urls:
                fs_dict = self.populate_dict(url, node_url, fs_dict)

        return fs_dict

    def populate_dict(self, fs_url, node_url, fs_dict):
        """
        Description:
            Function to populate a dictionary identifying the file systems
            below a node from properties passed from the parent function
            compile_dict.
        Args:
            fs_url (str): A url of the file system object.
            node_url (str): A url of the node object.
            dict (dict): The dictionary to be populated.
        Returns:
            dict. A dictionary of all the file systems below each node.
        """
        hostname = self.get_props_from_url(self.ms_node, node_url, 'hostname')

        if hostname not in fs_dict.keys():
            fs_dict[hostname] = {}
        stdout = \
        self.get_props_from_url(self.ms_node, fs_url)
        snap_external = 'false'
        if 'snap_external' in stdout:
            snap_external = \
            self.get_props_from_url(self.ms_node, fs_url, 'snap_external')
        if snap_external not in fs_dict[hostname].keys():
            fs_dict[hostname][snap_external] = {}
        stdout = self.get_props_from_url(self.ms_node, fs_url, 'type')
        if stdout not in fs_dict[hostname][snap_external].keys():
            fs_dict[hostname][snap_external][stdout] = []
        fs_dict[hostname][snap_external][stdout].append(fs_url)
        return fs_dict

    def get_all_file_sys_from_vol_grp(self, url):
        """
        Description:
            Function to retrieve all of the file system objects residing
            below the provided volume group object.
        Args:
            url (str): A url of the volume group object.
        Returns:
            list. A list of all the file system objects found.
        """
        stdout = self.find(self.ms_node, url, "file-system")
        return stdout

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
        return stdout

    def get_vol_grps_from_vxfs_fs_urls(self, urls):
        """
        Description:
            Function to return the url of the parent volume-group object of the
            supplied vxfs file-system object url.
        Args:
            urls (list): A list of LITP model urls of a vxfs file-system object
        Returns:
            list. A list of LITP model urls of the volume-group objects of the
                  supplied file-system urls.
        """
        vg_urls = []
        for url in urls:
            vg_urls.append(self.get_vol_grp_from_vxfs_fs_url(url))
        return vg_urls

    @staticmethod
    def get_vol_grp_from_vxfs_fs_url(url):
        """
        Description:
            Function to return the url of the parent volume-group object of the
            supplied vxfs file-system object url.
        Args:
            url (str): LITP model url of a vxfs file-system object.
        Returns:
            str. A LITP model url of the volume-group object of the supplied
                 file-system url.
        """
        split_url = url.split('/')
        return "/".join([split_url[model_id] for model_id in range(0, 7)])

    def create_phys_device(self, vol_grp_url, phys_dev_id, props):
        """
        Description:
            Function to create a physical-device object below the suppplied
            volume-group object.
        Args:
            vol_grp_url (str): LITP model url of a volume-group object.
            phys_dev_id (str): ID under which the physical-device is to be
                               created.
            props (str): Properties to be specified for the creation of the
                         physical-device.
        """
        phys_dev_url = vol_grp_url + "/physical_devices/" + phys_dev_id
        self.execute_cli_create_cmd(self.ms_node, phys_dev_url,
                                    "physical-device", props)
                                    #add_to_cleanup=False)

    def create_disk(self, url, props):
        """
        Description:
            Function to create a disk object below the supplied blade object
            url.
        Args:
            props (str): Properties with which the disk object should
                         be created.
        """
        self.execute_cli_create_cmd(self.ms_node, url + "/disks/test_disk",
                                    "disk", props)
                                    #add_to_cleanup=False)

    @staticmethod
    def convert_to_mb(size):
        """
        Converts given size to M
        """
        if "G" in size:
            value, _ = size.split("G")
            value = int(value)
            value *= 1024
            size = str(value)

        return size

    def get_vol_grp_disks(self, vol_grp_urls):
        """
        Description:
            Function to return a dictionary which lists all of the disk names
            which reside below each volume group.
        Args:
            all_vxfs_list (list): A list of LITP urls of all of the vxfs
                                  file-system objects found.
        Returns:
            vol_grp_disks. dict. A dictionary identifying the list of disk
                                 names which reside below each vxfs volume
                                 group.
        """
        vol_grp_disks = {}
        disk_names = []
        for vol_grp_url in vol_grp_urls:
            vol_grp_id = self.get_vol_grp_id_from_url(vol_grp_url)
            if vol_grp_id not in vol_grp_disks.keys():
                vol_grp_disks[vol_grp_id] = []
            phys_dev_urls = \
            self.find(self.ms_node, vol_grp_url, "physical-device",
                      assert_not_empty=False)
            for phys_dev_url in phys_dev_urls:
                disk_name = self.get_props_from_url(self.ms_node,
                                                    phys_dev_url,
                                                    "device_name")
                disk_names.append(disk_name)
                vol_grp_disks[vol_grp_id].append(disk_name)
        return vol_grp_disks

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
        split_url = url.split('/')
        return split_url[-1]

    def get_vol_grp_id_from_url(self, url):
        """
        Description:
            Function to return the file system id from the file system
            url supplied.
        Args:
            url (str): The url to a vol group.
        Returns:
            split_url. str. The vol group id.
        """
        return self.get_id_from_end_of_url(url)

    def get_all_vxfs_file_sys(self):
        """
        Description:
            Function to retrieve all the LITP urls of file-system objects of
            type vxfs.

            To ensure adequate file-system urls for test, snap_external
            property is updated to false.
        Returns:
            all_vxfs_list. list. Urls of all file-system objects of type vxfs.
        """
        file_sys_dict = self.retrieve_file_system_dict('vxvm')
        self.assertNotEqual({}, file_sys_dict)

        # FIND ALL FS WITH SNAP_EXTERNAL TRUE AND SET THEM TO FALSE
        # SO AS TO ENSURE PLENTY OF FS FOR TEST COVERAGE
        vxfs_url_list_1 = \
        self.compile_list_of_file_sys(file_sys_dict, fs_type='vxfs',
                                      search_type='true')

        for vx_url in list(set(vxfs_url_list_1)):
            self.backup_path_props(self.ms_node, vx_url)
            stdout, stderr, returnc = \
            self.execute_cli_update_cmd(self.ms_node, vx_url,
                                        props='snap_external=false')
            self.assertEqual([], stdout)
            self.assertEqual([], stderr)
            self.assertEqual(0, returnc)

        vxfs_url_list_2 = \
        self.compile_list_of_file_sys(file_sys_dict, fs_type='vxfs',
                                      search_type='false')

        for vx_url in vxfs_url_list_2:
            self.backup_path_props(self.ms_node, vx_url)
        all_vxfs_list = []
        all_vxfs_list.extend(list(set(vxfs_url_list_1)))
        all_vxfs_list.extend(list(set(vxfs_url_list_2)))
        self.assertNotEqual([], all_vxfs_list)

        return all_vxfs_list

    def get_sys_disks(self):
        """
        Description:
            Function to retrieve the name and size of all the disks configured
            on the nodes in the cluster
        Returns:
            disk_dict. dict. Key is disk name, value is disk size.
        """
        sys_urls = []
        for node_url in self.node_urls:
            sys_urls.append(self.deref_inherited_path(self.ms_node,
                                                      node_url + "/system")
                            )
        disk_dict = {}
        for sys_url in sys_urls:
            disk_urls = \
            self.find(self.ms_node, sys_url + "/disks", "disk",
                      assert_not_empty=False)
            for disk_url in disk_urls:
                disk_props = self.get_props_from_url(self.ms_node,
                                                     disk_url)
                if disk_props["name"] not in disk_dict.keys():
                    disk_dict[disk_props["name"]] = disk_props["size"]
                else:
                    self.assertEqual(disk_dict[disk_props["name"]],
                                     disk_props["size"])
        return disk_dict

    def get_vol_grp_maxsize(self, vol_grp_disks, disk_dict):
        """
        Description:
            Function to get the total size of all disks under each volume
            group in MB.
        Args:
            vol_grp_disks (dict): A collection of all the disks below each
                                  volume group.
            disk_dict (dict): A collection of disks and their sizes.
        Returns:
            dict. A dictionary of vol grp id as key, and total size as value.
        """

        # FIND THE SIZE OF ALL DISKS AND REPLACE IT IN THE DICTIONARY
        for vol_grp_disk in vol_grp_disks:
            disks = vol_grp_disks[vol_grp_disk]
            disk_sizes = []
            for disk in disks:
                mb_size = self.convert_to_mb(disk_dict[disk])
                disk_sizes.append(mb_size)
            vol_grp_disks[vol_grp_disk] = disk_sizes

        # TALLY THE SIZES BELOW EACH VOLUME GROUP
        for vol_grp_disk in vol_grp_disks:
            disk_sizes = vol_grp_disks[vol_grp_disk]
            total_size = 0
            for disk_size in disk_sizes:
                total_size += int(self.convert_to_mb(disk_size))
            vol_grp_disks[vol_grp_disk] = total_size

        return vol_grp_disks

    def verify_vxvm_phys_dev_deployment(self, vol_grp_maxsize,
                                        vol_grp_to_fs_dict):
        """
        Description:
            Function to verify the deployment of physical devices.
        Args:
            vol_grp_maxsize (dict): A dictionary identifying a volume group
                                    and its maximum size.
        """
        for vol_grp in vol_grp_maxsize.keys():

            node = self.get_active_node_for_vol_grp(vol_grp)
            vxvm_maxsize_rounded = \
            self.get_vol_grp_accum_phys_dev_size(node, vol_grp)
            disks_maxsize = vol_grp_maxsize[vol_grp]
            fs_sizes = vol_grp_to_fs_dict[vol_grp]
            total_fs_size = 0
            for fs_size in fs_sizes:
                total_fs_size += int(fs_size)
            total_fs_and_free_space = total_fs_size + int(vxvm_maxsize_rounded)
            self.assertEqual(disks_maxsize, total_fs_and_free_space)

    def get_free_disk_uuids(self, volg_grp_ids):
        """
        Description:
            Function to get the uuids for all of the shared disks in the
            cluster.
        Args:
            volg_grp_ids (list): A list of names of all of the volume group
                                 id's as they appear in the LITP model.
        Returns:
            dict. A dictionary with Key of the disk name and value of the uuid.
        """
        cmd = self.get_vxdisk_list("-e -o alldgs")
        nodes_disks = {}
        for node in self.test_nodes:
            stdout, _, _ = \
            self.run_command(node, cmd, su_root=True)
            nodes_disks[node] = stdout
        disks_list = self.compile_all_nodes_disks(nodes_disks, volg_grp_ids)

        # ASSERT THAT NO UNASSIGNED SHARED DISKS EXISTS.
        self.assertNotEqual({}, disks_list)
        shared_disks = set.intersection(disks_list)
        # shared_disks = set.intersection(*disks_list)
        # star gives a pylint error
        # untested without star wildcard.

        # ASSERT THAT AT LEAST ONE SHARED DISK HAS BEEN FOUND
        self.assertNotEqual(0, len(shared_disks))
        disk_names = \
        self.get_local_disks_from_vxvm_for_shared_disks(
                                            nodes_disks[self.test_nodes[0]],
                                            shared_disks)

        cmd = "/bin/ls -la /dev/disk/by-id"
        stdout, _, _ = self.run_command(self.test_nodes[0], cmd, su_root=True)
        relevant_entries = []
        relevant_dict = {}
        for line in stdout:
            for disk in disk_names:
                if re.search(disk + "$", line) and not re.search("wwn", line):
                    relevant_entries.append(line)
                    relevant_dict[line] = disk
        scsi_entries = []
        scsi_dict = {}
        for entry in relevant_entries:
            split_list = entry.split(" ")
            tidied_list = [x for x in split_list if x != ""]
            scsi_entries.append(tidied_list[8])
            scsi_dict[relevant_dict[entry]] = tidied_list[8]
        uuid_dict = {}
        for disk in scsi_dict.keys():
            uuid_dict[disk] = self.strip_uuid_from_scsi_entry(scsi_dict[disk])

        return uuid_dict

    @staticmethod
    def strip_uuid_from_scsi_entry(scsi_entry):
        """
        Description:
            Function to strip the uuid value used in LITP from the Linux
            disk representation.
        Args:
            scsi_entry (str): The Linux representation of the disk.
        Returns
            str. The uuid used by LITP during the disk object creation.
        """
        return scsi_entry.split("-")[1][1:]

    @staticmethod
    def get_local_disks_from_vxvm_for_shared_disks(vxdisk_console,
                                                   shared_disks):
        """
        Description:
            Function to strip the Linux disk names from the vxvm console.
        Args:
            vxdisk_console (list): The output of the vxdisk list console.
            shared_disks (list): The name of the shared disks as they appear
                                 in the vxdisk console output.
        Returns:
        list. The Linux names of the shared disks.
        """
        entries = [entry for entry in vxdisk_console
                   for disk in shared_disks if disk in entry]
        disk_names = []
        for entry in entries:
            split_list = entry.split(" ")
            tidied_list = [x for x in split_list if x != ""]
            disk_names.append(tidied_list[6])
        return disk_names

    @staticmethod
    def compile_all_nodes_disks(nodes_disks, vg_ids):
        """
        Description:
            Function to get all the disks on the nodes which have not
            been designated for use with LVM, or assigned to a vxvm volume
            group.
        Args:
            nodes_disks (list): List of all the shared disks.
            vg_ids (list): The ids of all the volume groups of type vxvm as
                           they appear in the LITP model.
        Returns:
            list. A list of disk names as they appear in the vxdisk console.
        """
        disks_list = []
        for node in nodes_disks.keys():
            disks = []
            node_disks = nodes_disks[node]
            for disk_entry in node_disks:
                if "DEVICE" in disk_entry or "LVM" in disk_entry:
                    continue
                # CHECK TO ENSURE THAT THE DISK ISN'T ALREADY ASSIGNED TO A
                # VOLUME GROUP.
                disk_assigned = [str for vg in vg_ids if vg in disk_entry]
                if disk_assigned != []:
                    continue
                disk = disk_entry.split(":")[0].split(' ')[0]
                if disk not in disks:
                    disks.append(disk)
            disks_list.append(set(disks))
        return disks_list

    @staticmethod
    def get_vxdisk_list(args=""):
        """
        Function to pass options to vxdisk list command.
        Args:
            args (str): The optional argument to be passed.
        Returns:
            str. The command with or without the optional argument.
        """
        return "/sbin/vxdisk {0} list".format(args)

    @staticmethod
    def get_vxdg_list(args=""):
        """
        Function to pass options to vxdg list command.
        Args:
            args (str): The optional argument to be passed.
        Returns:
            str. The command with or without the optional argument.
        """
        return "/sbin/vxdg {0} list".format(args)

    def get_active_node_for_vol_grp(self, vol_grp):
        """
        Function to get the active node from the output of vxdg command.
        Args:
            vol_grp (str): The volume group that used is searched for active
                           node.
        Returns:
            str. The name of the active node.
        """
        active_node_found = False
        self.test_nodes = self.get_managed_node_filenames()
        for node in self.test_nodes:
            stdout, _, _ = \
            self.run_command(node,
                                 self.get_vxdg_list(), su_root=True)
            updated_stdout = []
            for item in stdout:
                split_list_item = item.split(" ")
                updated_stdout.extend(split_list_item)
            if vol_grp in updated_stdout:
                return node
        self.assertTrue(active_node_found)

    @staticmethod
    def get_vxassist_maxsize_cmd(vol_grp, args=""):
        """
        Function to pass options to vxassist maxsize command.
        Args:
            vol_grp (str): The volume group used in vxassist.
            args (str): The optional arguments to be passed.
        Returns:
            str. The command with or without the optional argument.
        """
        return "/usr/sbin/vxassist -g {0} {1} maxsize".format(vol_grp, args)

    def get_vol_grp_accum_phys_dev_size(self, node, vol_grp):
        """
        Function to get the maximum size of volume group on physical devices.
        Args:
            node (str): The name of the active node
            vol_grp (str): The volume group that is to be searched.
        Returns:
            string representation the max volume group size in Gb.
        """
        stdout, _, _ = \
        self.run_command(node,
                    self.get_vxassist_maxsize_cmd(vol_grp), su_root=True)
        compile_obj = re.compile('[0-9]+Mb')
        match_str = compile_obj.search(stdout[0])
        compile_obj = re.compile('[0-9]+')
        match_str = compile_obj.match(match_str.group())
        size_in_mb = match_str.group()
        size_in_gb = float(size_in_mb) / 1024.0
        rounded_size = int(math.ceil(size_in_gb))
        mb_size = rounded_size * 1024
        # LITP allows only integer sizes
        return str(mb_size)

    @staticmethod
    def get_blockdevice_cmd(disk):
        """
        Description:
            Function to retrieve the block size of the provided disk.
        Args:
            disk (str): The Linux disk name.
        Returns:
            str. The block size of the specified disk.
        """
        return "/sbin/blockdev --getsize64 /dev/{0}".format(disk)

    def get_disk_size(self, node, disk_name):
        """
        Description:
            Function to get the size of the disk in MB.
        Args:
            node (str): Filename of the node on which the commands are to be
                        executed.
            disk_name (str) The Linux disk name for which the size is to be
                            retrieved.
        Returns:
            str. The size in MB of the specified disk.
        """
        cmd = self.get_blockdevice_cmd(disk_name)
        stdout, _, _ = self.run_command(node, cmd, su_root=True)
        return self.get_mb_size_from_blocks(stdout[0])

    @staticmethod
    def get_mb_size_from_blocks(blocks):
        """
        Description:
            Function to convert block size to MB.
        Args:
            blocks (str): The block size of a disk.
        Returns:
            str. The size of the blocks in MB.
        """
        return str(int(blocks) / 1024 / 1024)

    def get_vol_grp_file_sys_sizes(self, all_vxfs_list):
        """
        Description:
            Function to retrieve all the sizes of the file systems under
            each volume group.
        Args:
            all_vxfs_list (list): A list of all the file-system objects of
                                  type vxfs.
        """
        vol_grp_to_fs_dict = {}
        for vxfs in all_vxfs_list:
            props = self.get_props_from_url(self.ms_node, vxfs)
            size = props["size"]
            vol_grp_url = self.get_vol_grp_from_vxfs_fs_url(vxfs)
            vol_grp_id = self.get_vol_grp_id_from_url(vol_grp_url)
            if vol_grp_id not in vol_grp_to_fs_dict.keys():
                vol_grp_to_fs_dict[vol_grp_id] = []
            # CONVERT SIZE TO MB HERE SO AS TO USE COMMON UNIT FOR VERIFICATION
            size_mb = self.convert_to_mb(size)
            vol_grp_to_fs_dict[vol_grp_id].append(size_mb)
            if props["snap_external"] == "false" and props["snap_size"] != "0":
                snap_size = str(int(size_mb) * int(props["snap_size"]) / 100)
                vol_grp_to_fs_dict[vol_grp_id].append(snap_size)
        return vol_grp_to_fs_dict

    @attr('pre-reg', 'non-revert', 'story4331', 'story4331_tc02',
          'manual-test')
    def test_02_p_vxvm_multi_phys_dev_in_vg_expansion_remove(self):
        '''
        Description:
            To ensure that it is possible to, specify the creation of
            additional physical-device child objects below a volume group which
            resides below a storage-profile which has volume driver of type
            vxvm, which has already been deployed to a cluster. It also ensures
            that the snapshot creation/removal is not affected by this
            addition.
        Steps:
            1. Retrieve the current vxvm volume groups, their file systems,
               and physical devices.
            2. Ensure snapshots exist.
            3. Verify the configuration.
            4. Retrieve the uuid's of all free shared disks and their
               respective sizes.
            5. Create a new disk object.
            6. Below a storage-profile of volume_driver type vxvm which has
               already been deployed to the cluster, Specify the creation of an
               additional physical-device object which shall be created on the
               new disk object.
            7. Issue the create and run plan commands, and ensure that the
               deployment is successful.
            8. Verify the configuration.
            9. Issue the remove_snapshot command and ensure snapshots have
               been removed successfully.
            10. Issue the create_snapshot command and Ensure snapshots have
                been created successfully.
        '''
        # 1
        # FIND ALL OF THE FILE SYSTEMS OF TYPE VXFS
        all_vxfs_list = self.get_all_vxfs_file_sys()

        # 2
        # CREATE AND VERIFY SNAPSHOTS
        self.verify_snap_create_if_absent()
        self.verify_fs_cache(all_vxfs_list, "")

        # GET THE VOL GRP URLS FROM THE XVFS URLS
        vol_grp_urls = self.get_vol_grps_from_vxfs_fs_urls(all_vxfs_list)

        # RETRIEVE THE NAMES OF ALL OF THE DISK OBJECTS BELOW EACH VOLUME GROUP
        vol_grp_disks = self.get_vol_grp_disks(vol_grp_urls)

        # RETRIEVE THE VOL GRP IDS FROM THE FS URLS - USED WHEN COMPILING
        # LIST OF FREE SHARED DISKS
        # file_sys_ids = self.get_file_sys_ids_from_urls(all_vxfs_list)

        # RETRIEVE THE DISK OBJECTS FROM THE LITP MODEL
        # IDENTIFIES DISK NAME AND ITS SIZE
        disk_dict = self.get_sys_disks()

        # MAP THE VOLUME GROUP TO THE MAX STORAGE SPACE
        vol_grp_maxsize = self.get_vol_grp_maxsize(vol_grp_disks, disk_dict)

        vol_grp_file_sys_sizes = self.get_vol_grp_file_sys_sizes(all_vxfs_list)

        # 3
        # VERIFY THAT THE DEPLOYMENT OF THE SPECIFIED VOLUME GROUP, FILE-SYSTEM
        # AND PHYSICAL-DEVICES IS AS EXPECTED.
        self.verify_vxvm_phys_dev_deployment(vol_grp_maxsize,
                                             vol_grp_file_sys_sizes)

        # 4
        # GET THE UUIDS OF ALL OF THE UNALLOCATED SHARED DISKS
        uuids = self.get_free_disk_uuids(vol_grp_file_sys_sizes.keys())

        # ASSERT THAT SHARED DISKS EXIST.
        self.assertNotEqual({}, uuids)

        # GET THE SIZE OF ALL OF THE UNALLOCATED SHARED DISKS
        disk_size_dict = {}
        for disk_name in uuids:
            disk_size_dict[disk_name] = \
            self.get_disk_size(self.test_nodes[0], disk_name)

        # 5
        # CREATE THE SHARED DISK
        sys_urls = []
        for node_url in self.node_urls:
            sys_urls.append(self.deref_inherited_path(self.ms_node,
                                                      node_url + "/system"))
        uuid_disks = uuids.keys()
        uuid = uuids[uuid_disks[0]]
        mb_size = disk_size_dict[uuid_disks[0]]
        disk_name = "test_disk"

        for sys_url in sys_urls:
            self.create_disk(sys_url,
                             "uuid={0} name={1} size={2}M".format(uuid,
                                                                  disk_name,
                                                                  mb_size))

        # 6
        # CREATE THE ADDITIONAL PHYSICAL-DEVICE
        props = "device_name={0}".format(disk_name)
        phys_dev_id = "test_4331_02"
        self.create_phys_device(vol_grp_urls[0], phys_dev_id, props)

        # 7
        # DEPLOY THE NEW CONFIGURATION
        self.execute_cli_createplan_cmd(self.ms_node)
        self.execute_cli_runplan_cmd(self.ms_node)
        self.wait_for_plan_state(self.ms_node,
                                 test_constants.PLAN_COMPLETE)

        # 8
        # RETRIEVE THE NAMES OF ALL OF THE DISK OBJECTS BELOW EACH VOLUME GROUP
        vol_grp_disks = self.get_vol_grp_disks(vol_grp_urls)

        # RETRIEVE THE VOL GRP IDS FROM THE FS URLS - USED WHEN COMPILING
        # LIST OF FREE SHARED DISKS
        # file_sys_ids = self.get_file_sys_ids_from_urls(all_vxfs_list)

        # RETRIEVE THE DISK OBJECTS FROM THE LITP MODEL
        # IDENTIFIES DISK NAME AND ITS SIZE
        disk_dict = self.get_sys_disks()

        # MAP THE VOLUME GROUP TO THE MAX STORAGE SPACE
        vol_grp_maxsize = self.get_vol_grp_maxsize(vol_grp_disks, disk_dict)

        vol_grp_file_sys_sizes = self.get_vol_grp_file_sys_sizes(all_vxfs_list)

        # VERIFY THAT THE DEPLOYMENT OF THE SPECIFIED VOLUME GROUP, FILE-SYSTEM
        # AND PHYSICAL-DEVICES IS AS EXPECTED.
        self.verify_vxvm_phys_dev_deployment(vol_grp_maxsize,
                                             vol_grp_file_sys_sizes)

        # 9
        # REMOVE AND VERIFY SNAPSHOTS
        self.execute_cli_removesnapshot_cmd(self.ms_node)
        self.wait_for_plan_state(self.ms_node,
                                 test_constants.PLAN_COMPLETE)
        self.verify_fs_cache(all_vxfs_list, "", False)

        # 10
        # CREATE AND VERIFY SNAPSHOTS
        self.execute_cli_createsnapshot_cmd(self.ms_node)
        self.wait_for_plan_state(self.ms_node,
                                 test_constants.PLAN_COMPLETE)
        self.verify_fs_cache(all_vxfs_list, "")
