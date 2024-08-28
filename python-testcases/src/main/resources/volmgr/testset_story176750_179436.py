"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     March 2017
@author:    Laura Forbes
@summary:   TORF-176750
            As a LITP User I want to create named snapshots using the Volmgr
            Plugin when no more than 1 node has failed per cluster
            TORF-179436
            As a LITP User I want to remove named snapshots using the Volmgr
            Plugin when no more than 1 node has failed per cluster
            TORF-185287
            As a LITP User I want to create named VxVm snapshots using the
            Volmgr Plugin when no more than 1 node has failed per cluster
            TORF-185325
            As a LITP User I want to remove named VxVm snapshots using the
            Volmgr Plugin when no more than 1 node has failed per cluster

            The testing of these stories have been
            merged together as they go hand-in-hand.
"""
from litp_generic_test import GenericTest, attr
import test_constants as const
from storage_utils import StorageUtils
from vcs_utils import VCSUtils


class Story176750(GenericTest):
    """
        As a LITP User I want to create named snapshots using the Volmgr
        Plugin when no more than 1 node has failed per cluster
        As a LITP User I want to remove named snapshots using the Volmgr
        Plugin when no more than 1 node has failed per cluster
        As a LITP User I want to create named VxVm snapshots using the
        Volmgr Plugin when no more than 1 node has failed per cluster
        As a LITP User I want to remove named VxVm snapshots using the
        Volmgr Plugin when no more than 1 node has failed per cluster
    """

    def setUp(self):
        """ Runs before every single test """
        super(Story176750, self).setUp()

        self.storage = StorageUtils()
        self.vcs = VCSUtils()
        self.ms_node = self.get_management_node_filename()
        self.mn_nodes = self.get_managed_node_filenames()
        self.node_urls = self.find(self.ms_node, "/deployments", "node")
        self.snap_name = "ombs"
        self.offline_node = self.mn_nodes[0]
        self.offline_url = self.node_urls[0]
        self.offline_node_ilo_ip = self.get_node_ilo_ip(
            self.ms_node, self.offline_node)
        self.online_nodes = self.mn_nodes[1:]
        self.online_urls = self.node_urls[1:]

    def tearDown(self):
        """ Runs after every single test """
        super(Story176750, self).tearDown()

    def _extract_plan_data(self):
        """
        Description:
            Extract task URL paths and task descriptions from the plan.
        Returns:
            task_paths (list), task_descs (list): List of
                URL paths, and list of messages, in the plan.
        """
        plan_tasks = self.get_full_list_of_tasks(self.ms_node)
        task_paths = []
        task_descs = []
        for tasks in plan_tasks:
            # Save each tasks path
            task_paths.append(tasks['PATH'])
            # Save each task description
            task_descs.append(tasks['MESSAGE'])

        return task_paths, task_descs

    def _check_node_in_plan(self, node, plan_tasks, expect_present=True):
        """
        Description:
            Checks if a node is present in a plan. Can
            also assert that it is not present.
        Args:
            node (str): The node to check it's presence/absence.
            plan_tasks (list): List of URL paths in a plan.
        Kwargs:
            expect_present (bool): Whether the node is
                expected to be in the plan. Default is True.
        """
        if expect_present:
            self.assertTrue(any(node in x for x in plan_tasks),
                            "{0} not found in plan".format(node))
        else:
            self.assertFalse(any(node in x for x in plan_tasks),
                             "{0} unexpectedly found in plan".format(node))

    def _force_remove_snapshot(self):
        """
        Description:
            Forcefully removes the deployment
            snapshot and ombs snapshot, if present.
        """
        if self.is_snapshot_item_present(
                self.ms_node, snapshot_name=self.snap_name):
            self.execute_and_wait_removesnapshot(
                self.ms_node, args="-n {0} -f".format(self.snap_name))

        if self.is_snapshot_item_present(self.ms_node):
            self.execute_and_wait_removesnapshot(self.ms_node, args="-f")

    def _all_nodes_up(self, node_list, env='P'):
        """
        Description:
            Asserts that all nodes in the given list are pingable.
            If any node is not pingable, powering on the node is attempted.
        Args:
            node_list (list): List of nodes to check.
        Kwargs:
            env (str): 'P' if the environment is physical (default) or
                'C' if it is a cloud environment.
        """
        if env == 'P':
            for node in node_list:
                node_ip = self.get_node_ilo_ip(self.ms_node, node)
                if not self.is_ip_pingable(self.ms_node, node):
                    self.poweron_peer_node(self.ms_node, node, ilo_ip=node_ip)
        else:
            for node in node_list:
                if not self.is_ip_pingable(self.ms_node, node):
                    self.poweron_peer_node(self.ms_node, node)

    def _snap_action(self, action, exclude_node=None):
        """
        Description:
            Create or remove a snapshot, may specify to exclude nodes.
        Args:
            action (str): "Create" for create_snapshot, "Remove" for
                remove_snapshot. Strings are case-sensitive.
        Kwargs:
            exclude_node (str): Node(s) to be excluded.
        """
        # Argument(s) to pass to snapshot command, 'ombs' is snapshot name
        snap_args = "-n {0}".format(self.snap_name)
        if exclude_node:
            snap_args += " -e {0}".format(exclude_node)

        if action == "Create":
            self.execute_cli_createsnapshot_cmd(
                self.ms_node, snap_args, add_to_cleanup=False)
        elif action == "Remove":
            self.execute_cli_removesnapshot_cmd(
                self.ms_node, snap_args, add_to_cleanup=False)

    def _lvm_snap_exists(self, node_list, expect_positive=True):
        """
        Description:
            Checks whether named LVM snapshots exist on the given nodes.
            Does this by grepping for the snapshot name in
            the result of the lvs command from each node.
        Args:
            node_list (list): List of nodes to check.
        Kwargs:
            expect_positive (bool): Whether the snapshot is expected to exist.
        """
        # Execute a lvs command, grepping for the named snapshot
        lvs_grep = self.storage.get_lvs_cmd(grep_args=self.snap_name)

        for node in node_list:
            _, _, rc = self.run_command(node, lvs_grep, su_root=True)

            if expect_positive:
                # ombs snapshots should have been found so return code is 0
                self.assertEqual(0, rc, "{0} snapshot not found on node "
                                        "{1}".format(self.snap_name, node))
            else:
                # ombs snapshots should not have been found so rc is non-zero
                self.assertNotEqual(0, rc, "{0} unexpectedly found on node "
                                           "{1}".format(self.snap_name, node))

    def _check_lvm_task(self, node_list, task_descs, action):
        """
        Description:
            Asserts that there is a LVM task in
            the plan for each node in the given list.
        Args:
            node_list (list): List of nodes to assert are present.
            task_descs (list): List of task descriptions to check.
            action (str): "Create" for create_snapshot plans tasks, "Remove"
                for remove_snapshot plan tasks. Strings are case-sensitive.
        """
        snapshot_name = "L_vg1_root_{0}".format(self.snap_name)
        for node in node_list:
            self._check_node_in_plan(node, task_descs, True)
            lvm_task = '{0} LVM named backup snapshot "{1}" on ' \
                       'node "{2}"'.format(action, snapshot_name, node)
            self.assertTrue(any(lvm_task in x for x in task_descs))

    def _check_vxvm_task(self, vg_list, task_descs):
        """
        Description:
            Asserts that there is a VxVM task in the
            plan for each volume group in the given list.
        Args:
            vg_list (list): List of volume groups to check for.
            task_descs (list): List of task descriptions to check
        """
        for vg_name in vg_list:
            self.assertTrue(any(vg_name in x for x in task_descs),
                            "{0} not found in plan".format(vg_name))

    def _vxdg_list(self, node):
        """
        Description:
            Returns a list of volume groups enabled on the specified node.
            Returns None if no VGs are enabled.
        Args:
            node (str): Node to check what volume groups are enabled on it.
        """
        cmd = "/sbin/vxdg list"
        std_out, std_err, rc = self.run_command(node, cmd, su_root=True)
        self.assertEqual([], std_err)
        self.assertEqual(0, rc)

        volume_group_list = []
        if len(std_out) > 1:
            # If VGs found to be running on node
            for vol_grp in std_out[1:]:
                vg_name, state, _ = vol_grp.split()
                self.assertEqual("enabled", state,
                                 "Volume group {0} not in expected 'enabled' "
                                 "state on node {1}.".format(vg_name, node))
                if vg_name != "no_snap":
                    volume_group_list.append(vg_name)
        else:
            return None

        # Return the list of volume groups enabled on the
        # given node, omitting the description line in the output
        return volume_group_list

    def _get_all_volume_groups(self):
        """
        Description:
            Searches LITP Model for all volume group
            names and returns a sorted list of them.
        """
        all_vgs = []
        for filesys in self.get_all_volumes(self.ms_node):
            if filesys["volume_group_name"] != "no_snap":
                all_vgs.append(filesys["volume_group_name"])
        all_vgs.sort()
        return all_vgs

    def _check_vxvm_snaps_exist(self, node, volume_name, expect_present=True):
        """
        Description:
            Checks if a snapshot for the specified
            volume group exists on the given node.
        Args:
            node (str): Node to check if snapshot exists.
            volume_name (str): Volume group to check if snapshot exists for it.
        Kwargs:
            expect_present (bool): Whether the snapshot should be
                present on the given node. Default is True.
        """
        self.log('info', 'Verifying snapshot {0} exists on node {1} by '
                         'executing vxsnap list.'.format(volume_name, node))
        cmd = self.sto.get_vxsnap_cmd(volume_name, grep_args="L_")
        std_out, _, rc = self.run_command(node, cmd, su_root=True)
        if expect_present:
            self.assertEqual(0, rc)
            self.assertEqual(1, len(std_out))
        else:
            self.assertEqual(1, rc)
            self.assertEqual(0, len(std_out))

    def _find_vgs_on_nodes(self, node_list):
        """"
        Description:
            Return all volume groups that are active on the specified nodes.
        Args:
            node_list (list): List of nodes to check.
        Returns:
            list, dict: Sorted list of all enabled VGs, dictionary
                of nodes and the VGs enabled on them.
        """
        enabled_vgs = []  # List of all enabled VGs
        node_vgs = {}  # Dictionary of nodes and the VGs enabled on them
        for node in node_list:
            # Find the volume groups active on the node
            vol_grps_on_node = self._vxdg_list(node)
            if vol_grps_on_node:
                enabled_vgs.append(vol_grps_on_node)
                node_vgs[node] = vol_grps_on_node
        # Flatten list of lists to a list
        if enabled_vgs:
            enabled_vgs = [val for sublist in enabled_vgs for val in sublist]
            enabled_vgs.sort()

        return enabled_vgs, node_vgs

    def _check_service_parallel(self, node, service_name):
        """
        Description:
            Checks if the given service group is parallel or failover.
        Args:
            node (str): Node to run hagrp command on.
            service_name (str): Service group to check.
        Returns:
            (int): 1 if service group is a parallel service.
            (int): 0 if service group is a failover service.
        """
        parallel_cmd = self.vcs.get_hagrp_cmd(
                    '-display {0} | grep Parallel'.format(service_name))
        std_out, _, _ = self.run_command(node, parallel_cmd, su_root=True)
        value = std_out[0].split()[-1]
        self.assertTrue(value == '0' or value == '1',
                        "Unexpected Parallel value ({0}) returned for service "
                        "{1} on node {2}.".format(value, service_name, node))

        # Return 1 if parallel, 0 if failover
        return int(value)

    def _switch_services_one_node(self, online_node):
        """
        Description:
            Switches all failover services onto specified node.
        Args:
            online_node (str): Node to ensure all service groups are running on
        """
        # Ensure no SGs are in STARTING/STOPPING state
        self.wait_for_all_starting_vcs_groups(online_node)

        # Get list of all service groups
        hagrp_state_cmd = self.vcs.get_hagrp_state_cmd()
        service_groups, _, _ = self.run_command(
            online_node, hagrp_state_cmd, su_root=True)

        for group in service_groups[1:]:
            group_name, _, group_system, group_value = group.split()

            # Check if SG is failover
            if not self._check_service_parallel(online_node, group_name):
                # Check if the SG is active on the specified node
                if "ONLINE" in group_value and group_system != online_node:
                    # If the SG is not online on the specified
                    # node, switch it over to this node
                    switch_cmd = self.vcs.get_hagrp_cmd(
                        '-switch {0} -to {1}'.format(group_name, online_node))
                    self.run_command(online_node, switch_cmd, su_root=True)

        # Ensure no SGs are in STARTING/STOPPING state
        self.wait_for_all_starting_vcs_groups(online_node)

    @attr('kgb-physical', 'revert', 'story176750', 'story176750_tc06')
    def test_06_p_snap_healthy_nodes(self):
        """
            @tms_id: torf_176750_tc06
            @tms_requirements_id: TORF-176750
            @tms_title: Create/Remove snapshot without exclude
                nodes functionality and excluding healthy node
            @tms_description: When I issue the 'litp create_snapshot' command
            to create a named snapshot without specifying the exclude_nodes
            property, then the create named snapshot plan will include a
            "Create LVM named backup snapshot" task for all nodes in the
            deployment, plan runs to completion.
            When I issue the 'litp create_snapshot' command to create a named
            snapshot without specifying the exclude_nodes property and all
            nodes in the deployment are healthy, then the snapshot plan will
            include tasks to create VxVM named backup snapshots for VxVM based
            volume groups, plan runs to completion
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot without specifying the exclude_nodes property, then the
            remove named snapshot plan will include a "Remove LVM named backup
            snapshot" task for all nodes in the deployment, plan
            runs to completion.
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot without specifying the exclude_nodes property, then the
            snapshot plan will include tasks to remove VxVM named backup
            snapshots for VxVM based volume groups, plan runs to completion
            When I issue the 'litp create_snapshot' command
            to create a named snapshot with the exclude_nodes property
            containing a healthy node, then the snapshot plan will include
            tasks to create LVM named backup snapshots for LVM volumes for all
            peer nodes except for the excluded node and the plan runs
            successfully.
            When I issue the 'litp create_snapshot' command to create a named
            snapshot with the exclude_nodes property containing a healthy node
            where the VxVM filesystem is not mounted, then the snapshot plan
            will include tasks to create VxVM named backup snapshots for VxVM
            based volume groups and runs successfully.
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot with the exclude_nodes property containing a healthy node,
            then the snapshot plan will include tasks to remove LVM named
            backup snapshots for LVM volumes for all peer nodes except for the
            excluded node and the plan runs successfully.
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot with the exclude_nodes property containing a healthy node
            where a VxVM snapshot does not exist, then the snapshot plan will
            include tasks to remove VxVM named backup snapshots for VxVM based
            volume groups and runs to completion.
            @tms_test_steps:
                @step: Create snapshot not excluding any nodes
                @result: LVM snapshot created on all nodes
                @result: All volume group snapshots created
                @step: Remove snapshot not excluding any nodes
                @result: LVM snapshot removed from all nodes
                @result: All volume group snapshots removed
                @step: Create snapshot excluding a healthy node
                @result: Excluded node not referenced in plan
                @result: All included nodes referenced in plan and
                    there is a LVM task for each included node
                @result: LVM snapshot created on all included nodes
                @result: All volume group snapshots created on included node
                @step: Remove snapshot excluding a healthy node
                @result: Excluded node is not referenced in plan
                @result: All included nodes are referenced in plan and
                    there is a LVM task for each included node
                @result: LVM snapshot removed from all included nodes
                @result: All volume group snapshots removed from included node
            @tms_test_precondition: No named snapshot exists, all nodes up
            @tms_execution_type: Automated
        """
        self.log('info', '1. Ensure all nodes in the deployment are up.')
        self._all_nodes_up(self.mn_nodes)

        self.log('info', '2. Ensure no snapshot exists in the deployment.')
        self._force_remove_snapshot()

        self.log('info', '3. Get all volume group names from LITP Model.')
        all_vgs = self._get_all_volume_groups()

        # Get all enabled volume groups and map
        # them to the nodes that they are active on
        enabled_vgs, node_vgs = self._find_vgs_on_nodes(self.mn_nodes)
        self.log('info', '4. Assert that all VGs are enabled '
                         'on the node(s).')
        self.assertEqual(set(all_vgs), set(enabled_vgs))

        self.log('info', 'Beginning test for "Create snapshot legacy LVM".')

        self.log('info', '5. Create a snapshot not excluding any nodes.')
        self._snap_action("Create")

        # Extract paths and descriptions from the plan
        task_paths, task_descs = self._extract_plan_data()

        self.log('info', '6. Ensure that all nodes are referenced in the plan')
        for node in self.node_urls:
            self._check_node_in_plan(node, task_paths, True)

        self.log('info', '7. Check that the plan contains '
                         'a LVM task for each node.')
        self._check_lvm_task(self.mn_nodes, task_descs, action="Create")

        self.log('info', '8. Check that the plan contains a '
                         'VxVM task for each volume group.')
        self._check_vxvm_task(all_vgs, task_descs)

        self.log('info', '9. Ensure that the plan runs to completion.')
        self.assertEqual(True, self.wait_for_plan_state(
            self.ms_node, const.PLAN_COMPLETE),
                         "Plan did not complete successfully.")

        self.log('info', '10. Assert that LVM snapshot '
                         'was created on all nodes.')
        self._lvm_snap_exists(self.mn_nodes)

        self.log('info', '11. Assert that all VxVM snapshots were created.')
        for node, vol_grps in node_vgs.iteritems():
            for vol_grp in vol_grps:
                self._check_vxvm_snaps_exist(node, vol_grp)

        self.log('info', 'Beginning test for "Remove snapshot legacy LVM".')

        self.log('info', '12. Remove a snapshot not excluding any nodes.')
        self._snap_action("Remove")

        # Extract paths and descriptions from the plan
        task_paths, task_descs = self._extract_plan_data()

        self.log('info', '13. Ensure all nodes are referenced in the plan')
        for node in self.node_urls:
            self._check_node_in_plan(node, task_paths, True)

        self.log('info', '14. Check that the plan contains '
                         'a LVM task for each node.')
        self._check_lvm_task(self.mn_nodes, task_descs, action="Remove")

        self.log('info', '15. Check that the plan contains a '
                         'VxVM task for each volume group.')
        self._check_vxvm_task(all_vgs, task_descs)

        self.log('info', '16. Ensure that the plan runs to completion.')
        self.assertEqual(True, self.wait_for_plan_state(
            self.ms_node, const.PLAN_COMPLETE),
                         "Plan did not complete successfully.")

        self.log('info', '17. Assert that snapshot was removed from all nodes')
        self._lvm_snap_exists(self.mn_nodes, False)

        self.log('info', '18. Assert that all VxVM snapshots were removed.')
        for node, vol_grps in node_vgs.iteritems():
            for vol_grp in vol_grps:
                self._check_vxvm_snaps_exist(
                    node, vol_grp, expect_present=False)

        self.log('info', 'Beginning test for "Create snapshot excluding '
                         'healthy node where VxVM is not mounted".')

        self.log('info', '19. Switch all service groups to '
                         'a node that will not be excluded.')
        self._switch_services_one_node(self.online_nodes[0])

        self.log('info', '20. Get all enabled volume groups and map '
                         'them to the nodes that they are active on.')
        _, node_vgs = self._find_vgs_on_nodes(self.mn_nodes)
        self.log('info', '21. Ensure no volume groups are '
                         'active on the excluded node.')
        self.assertFalse(self.offline_node in node_vgs)
        self.log('info', '22. Ensure all volume groups are '
                         'active on the chosen online node.')
        self.assertEqual(set(all_vgs), set(node_vgs[self.online_nodes[0]]))

        self.log('info', '23. Create a snapshot excluding a healthy node.')
        self._snap_action("Create", self.offline_node)

        # Extract paths and descriptions from the plan
        task_paths, task_descs = self._extract_plan_data()

        self.log('info', '24. Ensure that the excluded node is '
                         'not referenced in the plan.')
        self._check_node_in_plan(self.offline_url, task_paths, False)
        self._check_node_in_plan(self.offline_node, task_descs, False)

        self.log('info', '25. Ensure that the included '
                         'nodes are referenced in the plan.')
        for node in self.node_urls[1:]:
            self._check_node_in_plan(node, task_paths, True)

        self.log('info', '26. Check that the plan contains '
                         'a LVM task for each included node.')
        self._check_lvm_task(self.mn_nodes[1:], task_descs, "Create")

        self.log('info', '27. Check that the plan contains a '
                         'VxVM task for each volume group.')
        self._check_vxvm_task(all_vgs, task_descs)

        self.log('info', '28. Ensure that the plan runs to completion.')
        self.assertEqual(True, self.wait_for_plan_state(
            self.ms_node, const.PLAN_COMPLETE),
                         "Plan did not complete successfully.")

        self.log('info', '29. Assert that LVM snapshot was '
                         'created on all included nodes.')
        self._lvm_snap_exists(self.mn_nodes[1:])

        self.log('info', '30. Assert that all VxVM snapshots were created.')
        for vol_grp in node_vgs[self.online_nodes[0]]:
            self._check_vxvm_snaps_exist(self.online_nodes[0], vol_grp)

        self.log('info', 'Beginning test for "Remove snapshot excluding '
                         'healthy node where VxVM is not mounted".')

        self.log('info', '31. Remove a snapshot excluding a healthy node.')
        self._snap_action("Remove", self.offline_node)

        # Extract paths and descriptions from the plan
        task_paths, task_descs = self._extract_plan_data()

        self.log('info', '32. Ensure that the excluded node is '
                         'not referenced in the plan.')
        self._check_node_in_plan(self.offline_url, task_paths, False)
        self._check_node_in_plan(self.offline_node, task_descs, False)

        self.log('info', '33. Ensure that the included '
                         'nodes are referenced in the plan.')
        for node in self.node_urls[1:]:
            self._check_node_in_plan(node, task_paths, True)

        self.log('info', '34. Check that the plan contains '
                         'a LVM task for each included node.')
        self._check_lvm_task(self.mn_nodes[1:], task_descs, "Remove")

        self.log('info', '35. Check that the plan contains a '
                         'VxVM task for each volume group.')
        self._check_vxvm_task(all_vgs, task_descs)

        self.log('info', '36. Ensure that the plan runs to completion.')
        self.assertEqual(True, self.wait_for_plan_state(
            self.ms_node, const.PLAN_COMPLETE),
                         "Plan did not complete successfully.")

        self.log('info', '37. Assert that LVM snapshot was '
                         'removed from all included nodes.')
        self._lvm_snap_exists(self.mn_nodes[1:], False)

        self.log('info', '38. Assert that all VxVM snapshots were removed.')
        for vol_grp in node_vgs[self.online_nodes[0]]:
            self._check_vxvm_snaps_exist(
                self.online_nodes[0], vol_grp, expect_present=False)

    @attr('all', 'revert', 'story176750', 'story176750_tc10')
    def test_10_p_snap_non_ssh_node(self):
        """
            @tms_id: torf_176750_tc10
            @tms_requirements_id: TORF-176750
            @tms_title: Exclude healthy node where SSH is down
            @tms_description: When I issue the 'litp create_snapshot' command
            to create a named snapshot with the exclude_nodes property
            containing a healthy node but it is not contactable via SSH, then
            the snapshot plan will include tasks to create LVM named backup
            snapshots for LVM volumes for all peer nodes except for the
            excluded node and the plan runs successfully
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot with the exclude_nodes property containing a healthy node
            but it is not contactable via SSH, then the snapshot plan will
            include tasks to remove LVM named backup snapshots for LVM volumes
            for all peer nodes except for the excluded node and
            the plan runs successfully
            @tms_test_steps:
                @step: Turn off SSH on one node
                @result: SSH daemon is no longer running on node
                @step: Move sshd on the node so Puppet can't restart it
                @result: sshd moved to /home/litp-admin folder
                @step: Create snapshot excluding the non-SSHable node
                @result: Non-SSHable node is not referenced in plan
                @result: Plan contains LVM task for each SSHABLE node
                @result: Snapshot created on all SSHable nodes
                @step: Remove snapshot excluding the non-SSHable node
                @result: non-SSHable node is not referenced in plan
                @result: Plan contains LVM task for each SSHABLE node
                @result: Snapshot removed from all SSHable nodes
                @step: Move sshd back to /usr/sbin/
                @result: sshd moved to /usr/sbin/ successfully
            @tms_test_precondition: No named snapshot exists,
                SSH is down on one node
            @tms_execution_type: Automated
        """
        self.log('info', '1. Ensure all nodes in the deployment are up.')
        self._all_nodes_up(self.mn_nodes, env='C')

        self.log('info', '2. Ensure no snapshot exists in the deployment.')
        self._force_remove_snapshot()

        try:
            self.log('info', 'Beginning create_snapshot test.')

            self.log('info', '3. Turn off SSH on one node.')
            self.stop_service(self.offline_node, "sshd")

            self.log('info', '4. Move sshd on the node so '
                             'that Puppet can\'t restart it.')
            self.mv_file_on_node(self.offline_node, "/usr/sbin/sshd",
                                 "/home/litp-admin", su_root=True)

            self.log('info', '5. Create a snapshot excluding '
                             'the non-SSHable node.')
            self._snap_action("Create", self.offline_node)

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '6. Ensure that the non-SSHable node is '
                             'not referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, False)
            self._check_node_in_plan(self.offline_node, task_descs, False)

            self.log('info', '7. Ensure that the SSHABLE '
                             'nodes are referenced in the plan.')
            for node in self.node_urls[1:]:
                self._check_node_in_plan(node, task_paths, True)

            self.log('info', '8. Check that the plan contains '
                             'a LVM task for each SSHABLE node.')
            self._check_lvm_task(self.mn_nodes[1:], task_descs, "Create")

            self.log('info', '9. Ensure that the plan runs to completion.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_COMPLETE),
                             "Plan did not complete successfully.")

            self.log('info', '10. Assert that snapshot was '
                             'created on all SSHable nodes.')
            self._lvm_snap_exists(self.mn_nodes[1:])

            self.log('info', 'Beginning remove_snapshot test.')

            self.log('info', '11. Remove a snapshot excluding '
                             'the non-SSHable node.')
            self._snap_action("Remove", self.offline_node)

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '12. Ensure that the non-SSHable node is '
                             'not referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, False)
            self._check_node_in_plan(self.offline_node, task_descs, False)

            self.log('info', '13. Ensure that the SSHABLE '
                             'nodes are referenced in the plan.')
            for node in self.node_urls[1:]:
                self._check_node_in_plan(node, task_paths, True)

            self.log('info', '14. Check that the plan contains '
                             'a LVM task for each SSHABLE node.')
            self._check_lvm_task(self.mn_nodes[1:], task_descs, "Remove")

            self.log('info', '15. Ensure that the plan runs to completion.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_COMPLETE),
                             "Plan did not complete successfully.")

            self.log('info', '16. Assert that snapshot was '
                             'removed from all SSHABLE nodes.')
            self._lvm_snap_exists(self.mn_nodes[1:], False)
        finally:
            self.log('info', '17. Move sshd back to /usr/sbin/ '
                             'so that Puppet can restart it.')
            self.mv_file_on_node(self.offline_node, "/home/litp-admin/sshd",
                                 "/usr/sbin", su_root=True)

    @attr('kgb-physical', 'revert', 'story176750', 'story176750_tc17')
    def test_17_p_lvm_vxvm_task_fails(self):
        """
            @tms_id: torf_176750_tc17
            @tms_requirements_id: TORF-176750
            @tms_title: LVM Task Fails, VxVM Task Fails
            @tms_description: When I issue the 'litp create_snapshot' command
            to create a named snapshot with the exclude_nodes property
            containing nodeX but an LVM task for included nodeY fails, then
            the snapshot plan fails
            When I issue the 'litp create_snapshot' command to create a named
            snapshot with the exclude_nodes property containing nodeX but a
            VxVM task for included nodeY fails, then the snapshot plan fails
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot with the exclude_nodes property containing nodeX but an
            LVM task for included nodeY fails, then the snapshot plan fails
            When I issue the 'litp remove_snapshot' command to remove a named
            snapshot with the exclude_nodes property containing nodeX but a
            VxVM task for included nodeY fails, then the snapshot plan fails
            @tms_test_steps:
                @step: Move lvcreate on node so LVM creation task fails
                @result: lvcreate moved to /home/litp-admin folder
                @step: Create snapshot excluding healthy node
                @result: Corrupted node referenced in plan
                @result: Plan contains LVM task for corrupted node
                @result: Healthy node is not referenced in the plan
                @result: Plan fails
                @step: Move lvcreate back to /sbin/
                @result: lvcreate moved to /sbin/ successfully
                @step: Remove faulty snapshot
                @result: Snapshot successfully removed
                @step: Move vxsnap on node so VxVM creation task fails
                @result: vxsnap moved to /home/litp-admin folder
                @step: Create snapshot excluding healthy node
                @result: Plan fails
                @step: Move vxsnap back to /sbin/
                @result: vxsnap moved to /sbin/ successfully
                @step: Create snapshot on all nodes for test
                @result: Snapshot successfully created on all nodes
                @step: Move lvremove on node so LVM removal task fails
                @result: lvremove moved to /sbin/ successfully
                @step: Remove snapshot excluding healthy node
                @result: Corrupted node referenced in plan
                @result: Plan contains LVM task for corrupted node
                @result: Healthy node is not referenced in the plan
                @result: Plan fails
                @step: Move lvremove back to /sbin/
                @result: lvremove moved to /sbin/ successfully
                @step: Remove snapshot from all nodes
                @result: Snapshots removed successfully
                @step: Create snapshot on all nodes for test
                @result: Snapshot successfully created on all nodes
                @step: Move vxsnap on node so VxVM removal task fails
                @result: vxsnap moved to /sbin/ successfully
                @step: Remove snapshot excluding healthy node
                @result: Plan fails
                @step: Move vxsnap back to /sbin/
                @result: vxsnap moved to /sbin/ successfully
                @step: Remove snapshot from all nodes
                @result: Snapshots removed successfully
            @tms_test_precondition: No named snapshot exists
            @tms_execution_type: Automated
        """
        self.log('info', '1. Ensure all nodes in the deployment are up.')
        self._all_nodes_up(self.mn_nodes)

        self.log('info', '2. Ensure no snapshot exists in the deployment.')
        self._force_remove_snapshot()

        try:
            self.log('info', 'Beginning create_snapshot LVM test.')

            self.log('info', '3. Move lvcreate on the node so '
                             'that the LVM creation task fails.')
            self.mv_file_on_node(self.offline_node, "/sbin/lvcreate",
                                 "/home/litp-admin", su_root=True)

            self.log('info', '4. Create a snapshot excluding a healthy node.')
            self._snap_action("Create", self.mn_nodes[1])

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '5. Ensure that the corrupted '
                             'node is referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, True)
            self._check_node_in_plan(self.offline_node, task_descs, True)

            self.log('info', '6. Check that the plan contains '
                             'a LVM task for the corrupted node.')
            self._check_lvm_task([self.offline_node], task_descs, "Create")

            self.log('info', '7. Ensure that the healthy node is '
                             'not referenced in the plan.')
            self._check_node_in_plan(self.node_urls[1], task_paths, False)
            self._check_node_in_plan(self.mn_nodes[1], task_descs, False)

            self.log('info', '8. Ensure that the plan fails.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_FAILED),
                             "Plan not in expected 'Failed' state.")

        finally:
            self.log('info', '9. Move lvcreate back to /sbin/')
            self.mv_file_on_node(self.offline_node,
                                 "/home/litp-admin/lvcreate", "/sbin",
                                 su_root=True)

            self.log('info', '10. Remove the faulty snapshot.')
            self.execute_and_wait_removesnapshot(
                self.ms_node, args="-n {0} -f".format(self.snap_name))

        try:
            self.log('info', 'Beginning create_snapshot VxVM test.')

            self.log('info', '11. Ensuring at least one volume '
                             'group is active on the included node.')
            if self._vxdg_list(self.online_nodes[0]) is None:
                self._switch_services_one_node(self.online_nodes[0])

            self.log('info', '12. Move vxsnap on the node so '
                             'that the VxVM creation task fails.')
            self.mv_file_on_node(self.online_nodes[0], "/sbin/vxsnap",
                                 "/home/litp-admin", su_root=True)

            self.log('info', '13. Create a snapshot excluding a healthy node.')
            self._snap_action("Create", self.offline_node)

            self.log('info', '14. Ensure that the plan fails on a VxVM task.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_FAILED),
                             "Plan not in expected 'Failed' state.")

            failed_list = self.get_plan_task_states(
                self.ms_node, const.PLAN_TASKS_FAILED)
            for task in failed_list:
                self.assertTrue(
                    "Create VxVM named backup snapshot" in task['MESSAGE'])

        finally:
            self.log('info', '15. Move vxsnap back to /sbin/')
            self.mv_file_on_node(self.online_nodes[0],
                                 "/home/litp-admin/vxsnap", "/sbin",
                                 su_root=True)

            self.log('info', '16. Remove the faulty snapshot.')
            self.execute_and_wait_removesnapshot(
                self.ms_node, args="-n {0} -f".format(self.snap_name))

        try:
            self.log('info', 'Beginning remove_snapshot LVM test.')
            self.log('info', '17. Create a snapshot on all nodes for test.')
            self.execute_and_wait_createsnapshot(
                self.ms_node, args="-n {0}".format(self.snap_name))

            self.log('info', '18. Move lvremove on the node so '
                             'that the LVM removal task fails.')
            self.mv_file_on_node(self.offline_node, "/sbin/lvremove",
                                 "/home/litp-admin", su_root=True)

            self.log('info', '19. Remove a snapshot excluding a healthy node.')
            self._snap_action("Remove", self.mn_nodes[1])

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '20. Ensure that the corrupted '
                             'node is referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, True)
            self._check_node_in_plan(self.offline_node, task_descs, True)

            self.log('info', '21. Check that the plan contains '
                             'a LVM task for the corrupted node.')
            self._check_lvm_task([self.offline_node], task_descs, "Remove")

            self.log('info', '22. Ensure that the healthy node is '
                             'not referenced in the plan.')
            self._check_node_in_plan(self.node_urls[1], task_paths, False)
            self._check_node_in_plan(self.mn_nodes[1], task_descs, False)

            self.log('info', '23. Ensure that the plan fails.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_FAILED),
                             "Plan not in expected 'Failed' state.")
        finally:
            self.log('info', '24. Move lvremove back to /sbin/')
            self.mv_file_on_node(self.offline_node,
                                 "/home/litp-admin/lvremove",
                                 "/sbin", su_root=True)

            self.log('info', '25. Remove the snapshots created for this test.')
            self.execute_and_wait_removesnapshot(
                self.ms_node, args="-n {0} -f".format(self.snap_name))

        try:
            self.log('info', 'Beginning remove_snapshot VxVM test.')
            self.log('info', '26. Create a snapshot on all nodes for test.')
            self.execute_and_wait_createsnapshot(
                self.ms_node, args="-n {0}".format(self.snap_name))

            self.log('info', '27. Ensuring at least one volume '
                             'group is active on the included node.')
            if self._vxdg_list(self.online_nodes[0]) is None:
                self._switch_services_one_node(self.online_nodes[0])

            self.log('info', '28. Move vxsnap on the node so '
                             'that the VxVM removal task fails.')
            self.mv_file_on_node(self.online_nodes[0], "/sbin/vxsnap",
                                 "/home/litp-admin", su_root=True)

            self.log('info', '29. Remove a snapshot excluding a healthy node.')
            self._snap_action("Remove", self.offline_node)

            self.log('info', '30. Ensure that the plan fails.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_FAILED),
                             "Plan not in expected 'Failed' state.")

            failed_list = self.get_plan_task_states(
                self.ms_node, const.PLAN_TASKS_FAILED)
            for task in failed_list:
                self.assertTrue(
                    "Remove VxVM named backup snapshot" in task['MESSAGE'])
        finally:
            self.log('info', '31. Move vxsnap back to /sbin/')
            self.mv_file_on_node(self.online_nodes[0],
                                 "/home/litp-admin/vxsnap", "/sbin",
                                 su_root=True)

            self.log('info', '32. Remove the snapshots created for this test.')
            self.execute_and_wait_removesnapshot(
                self.ms_node, args="-n {0} -f".format(self.snap_name))

    @attr('kgb-physical', 'revert', 'story176750', 'story176750_tc21')
    def test_21_n_no_snapshot_exists(self):
        """
            @tms_id: torf_176750_tc21
            @tms_requirements_id: TORF-176750
            @tms_title: Exclude node, no snapshot exists
            @tms_description: When I issue the 'litp remove_snapshot' command
            to remove a named snapshot with the exclude_nodes property
            containing any node but the named snapshot does not exist, then the
            snapshot plan does not get created and a "DoNothingPlanError"
            error is reported
            @tms_test_steps:
                @step: Ensure that no named snapshot exists
                @result: No named snapshot exists
                @step: Run the remove_snapshot command excluding a node and
                    specifying a named snapshot that does not exist
                @result: "DoNothingPlanError" error returned
                @result: Plan not created
            @tms_test_precondition: No named snapshot exists
            @tms_execution_type: Automated
        """
        self.log('info', '1. Ensure all nodes in the deployment are up.')
        self._all_nodes_up(self.mn_nodes)

        self.log('info', '2. Ensure no snapshot exists in the deployment.')
        self._force_remove_snapshot()

        self.log('info', "3. Run the remove_snapshot command excluding "
                         "a node and specifying a named snapshot that"
                         " doesn't exist.")
        snap_args = "-n {0} -e {1}".format(self.snap_name, self.offline_node)
        std_out, std_err, rc = self.execute_cli_removesnapshot_cmd(
            self.ms_node, snap_args,
            expect_positive=False, add_to_cleanup=False)

        self.log('info', "4. Assert that a DoNothingPlanError was returned.")
        self.assertEqual([], std_out)
        self.assertEqual(1, rc)
        self.assertTrue(any("DoNothingPlanError" in x for x in std_err))

    @attr('kgb-physical', 'revert', 'story176750', 'story176750_tc01')
    def test_01_p_snap_faulty_node(self):
        """
            @tms_id: torf_176750_tc01
            @tms_requirements_id: TORF-176750
            @tms_title: Snapshot exclude faulty node, include faulty node
            @tms_description: When I issue the 'litp create_snapshot'
                command to create a named snapshot with the exclude_nodes
                property containing a faulted node, then the snapshot plan will
                include tasks to create LVM named backup snapshots for LVM
                volumes for all peer nodes except for the excluded node and
                the plan runs successfully
                When I issue the 'litp create_snapshot' command to create a
                named snapshot with the exclude_nodes property containing a
                faulted node, then the snapshot plan will include tasks to
                create VxVM named backup snapshots for VxVM based volume
                groups and runs successfully
                When I issue the 'litp remove_snapshot' command to remove a
                named snapshot with the exclude_nodes property containing a
                faulted node, then the snapshot plan will include tasks to
                remove LVM named backup snapshots for LVM volumes for all peer
                nodes except for the excluded node, the plan runs successfully
                and snapshots are removed from all nodes except
                the excluded node
                When I issue the 'litp remove_snapshot' command to remove a
                named snapshot with the exclude_nodes property containing a
                faulted node, then the snapshot plan will include tasks to
                remove VxVM named backup snapshots for VxVM based volume
                groups and runs successfully
                When I issue the 'litp create_snapshot' command
                to create a named snapshot without specifying the exclude_nodes
                property but a node is faulted, then the create named snapshot
                plan will include a "Create LVM named backup snapshot" task for
                all nodes in the deployment INCLUDING the faulted node,
                plan fails
                When I issue the 'litp remove_snapshot' command to remove a
                named snapshot without specifying the exclude_nodes property
                but a node is faulted, then the remove named snapshot plan
                will include a "Remove LVM named backup snapshot" task for all
                nodes in the deployment INCLUDING the faulted node,
                the plan will fail
                When I issue the 'litp remove_snapshot' command to remove a
                named snapshot with the force option and without specifying the
                exclude_nodes property but a node is down, then the snapshot
                plan runs to completion removing snapshots from all
                nodes except for the faulted node
            @tms_test_steps:
                @step: Power off a node in the deployment
                @result: Node successfully powers off
                @step: Create snapshot excluding the offline node
                @result: Offline node not referenced in plan
                @result: Healthy nodes referenced in plan
                @result: Plan contains a LVM task for each healthy node
                @result: Plan contains a VxVM task for each volume group
                @result: Plan runs to completion
                @result: Snapshot created on all healthy nodes
                @result: All volume group snapshots created
                @step: Remove snapshot excluding the offline node
                @result: Offline node not referenced in plan
                @result: Healthy nodes referenced in plan
                @result: Plan contains a LVM task for each healthy node
                @result: Plan contains a VxVM task for each volume group
                @result: Plan runs to completion
                @result: Snapshot removed from all healthy nodes
                @result: All volume group snapshots removed
                @step: Create snapshot not excluding the offline node
                @result: Offline node is referenced in plan
                @result: Plan fails
                @step: Remove snapshot not excluding the offline node
                @result: Offline node is referenced in plan
                @result: Plan fails
                @step: Force remove the snapshot not excluding any nodes
                @result: Snapshot successfully removed
                @step: Power on offlined node
                @result: Node successfully powered on
            @tms_test_precondition: A node in the deployment is powered off,
                no named snapshot exists
            @tms_execution_type: Automated
        """
        self.log('info', '1. Ensure no snapshot exists in the deployment.')
        self._force_remove_snapshot()

        self.log('info', '2. Get all volume group names from LITP Model.')
        all_vgs = self._get_all_volume_groups()

        try:
            self.log('info', '3. Ensure a node in the deployment is offline.')
            if self.is_ip_pingable(self.ms_node, self.offline_node):
                self.poweroff_peer_node(self.ms_node, self.offline_node,
                                        ilo_ip=self.offline_node_ilo_ip)

            self.log('info', '4. Ensure all other nodes in deployment are up.')
            self._all_nodes_up(self.mn_nodes[1:])

            # Get all enabled volume groups and map
            # them to the nodes that they are active on
            enabled_vgs, node_vgs = self._find_vgs_on_nodes(self.online_nodes)
            self.log('info', '5. Assert that all VGs are enabled '
                             'on the online node(s).')
            self.assertEqual(all_vgs, enabled_vgs)

            self.log('info', 'Beginning create_snapshot '
                             'test excluding offline node.')

            self.log('info', '6. Create a snapshot excluding the offline node')
            self._snap_action("Create", self.offline_node)

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '7. Ensure that the offline node is '
                             'not referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, False)
            self._check_node_in_plan(self.offline_node, task_descs, False)

            self.log('info', '8. Ensure that the healthy '
                             'nodes are referenced in the plan.')
            for node in self.node_urls[1:]:
                self._check_node_in_plan(node, task_paths, True)

            self.log('info', '9. Check that the plan contains '
                             'a LVM task for each healthy node.')
            self._check_lvm_task(self.mn_nodes[1:], task_descs, "Create")

            self.log('info', '10. Check that the plan contains a '
                             'VxVM task for each volume group.')
            for vg_name in all_vgs:
                self.assertTrue(any(vg_name in x for x in task_descs),
                                "{0} not found in plan".format(vg_name))

            self.log('info', '11. Ensure that the plan runs to completion.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_COMPLETE),
                             "Plan did not complete successfully.")

            self.log('info', '12. Assert that LVM snapshot was '
                             'created on all healthy nodes.')
            self._lvm_snap_exists(self.mn_nodes[1:])

            self.log('info', '13. Assert that all VxVM snapshots were created')
            for node, vol_grps in node_vgs.iteritems():
                for vol_grp in vol_grps:
                    self._check_vxvm_snaps_exist(node, vol_grp)

            self.log('info', 'Beginning remove_snapshot '
                             'test excluding offline node.')

            self.log('info', '14. Remove a snapshot '
                             'excluding the offline node.')
            self._snap_action("Remove", self.offline_node)

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '15. Ensure that the offline node is '
                             'not referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, False)
            self._check_node_in_plan(self.offline_url, task_descs, False)

            self.log('info', '16. Ensure that the healthy '
                             'nodes are referenced in the plan.')
            for node in self.node_urls[1:]:
                self._check_node_in_plan(node, task_paths, True)

            self.log('info', '17. Check that the plan contains '
                             'a LVM task for each healthy node.')
            self._check_lvm_task(self.mn_nodes[1:], task_descs, "Remove")

            self.log('info', '18. Check that the plan contains a '
                             'VxVM task for each volume group.')
            for vg_name in all_vgs:
                self.assertTrue(any(vg_name in x for x in task_descs),
                                "{0} not found in plan".format(vg_name))

            self.log('info', '19. Ensure that the plan runs to completion.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_COMPLETE),
                             "Plan did not complete successfully.")

            self.log('info', '20. Assert that LVM snapshot was '
                             'removed from all healthy nodes.')
            self._lvm_snap_exists(self.mn_nodes[1:], False)

            self.log('info', '21. Assert that all VxVM snapshots were removed')
            for node, vol_grps in node_vgs.iteritems():
                for vol_grp in vol_grps:
                    self._check_vxvm_snaps_exist(
                        node, vol_grp, expect_present=False)

            self.log('info', 'Beginning create_snapshot '
                             'test not excluding offline node.')

            self.log('info', '22. Create a snapshot not '
                             'excluding the offline node.')
            self._snap_action("Create")

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '23. Ensure that the offline node is '
                             'referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, True)
            self._check_node_in_plan(self.offline_node, task_descs, True)

            self.log('info', '24. Ensure that the plan fails.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_FAILED),
                             "Plan not in expected 'Failed' state.")

            self.log('info', 'Beginning remove_snapshot '
                             'test not excluding offline node.')
            self.log('info', '25. Remove a snapshot not '
                             'excluding the offline node.')
            self._snap_action("Remove")

            # Extract paths and descriptions from the plan
            task_paths, task_descs = self._extract_plan_data()

            self.log('info', '26. Ensure that the offline node is '
                             'referenced in the plan.')
            self._check_node_in_plan(self.offline_url, task_paths, True)
            self._check_node_in_plan(self.offline_node, task_descs, True)

            self.log('info', '27. Ensure that the plan fails.')
            self.assertEqual(True, self.wait_for_plan_state(
                self.ms_node, const.PLAN_FAILED),
                             "Plan not in expected 'Failed' state.")

        finally:
            self.log('info', 'Beginning force remove_snapshot test.')
            self.log('info', '28. Force remove the '
                             'snapshot not excluding any nodes.')
            self._force_remove_snapshot()

            self.log('info', '29. Power on the offlined node.')
            self.poweron_peer_node(self.ms_node, self.offline_node,
                                   ilo_ip=self.offline_node_ilo_ip)
