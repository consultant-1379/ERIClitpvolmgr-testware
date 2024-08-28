"""
COPYRIGHT Ericsson 2019
The copyright to the computer program(s) herein is the property of
Ericsson Inc. The programs may be used and/or copied only with written
permission from Ericsson Inc. or in accordance with the terms and
conditions stipulated in the agreement/contract under which the
program(s) have been supplied.

@since:     January 2016
@author:    Maurizio Senno
@summary:   LITPCDS-11872
            As a LITP user I want to be able to control the order in which
            clusters are rebooted during a snapshot restore plan so that
            I can ensure my deployment successfully starts up
            LITPCDS-13469
            No VCS stop task shoudl be generated for a cluster that was not
            present at snapshot creation time
"""
import os
import re
from litp_generic_test import GenericTest, attr
from redhat_cmd_utils import RHCmdUtils
import test_constants


class Story11872(GenericTest):
    """
        As a LITP user I want to be able to control the order in which
        clusters are rebooted during a snapshot restore plan so
        I can ensure my deployment successfully starts up
    """

    def setUp(self):
        """Setup variables for every test"""
        # 1. Call super class setup
        super(Story11872, self).setUp()
        # 2. Set up variables used in the test
        self.ms1 = self.get_management_node_filename()
        self.rhcmd = RHCmdUtils()
        self.dummy_package = 'ERIClitpstory11872_CXP1234567'
        self.dummy_rpm = ('{0}-1.0.1-SNAPSHOT20160115113155.noarch.rpm'.
                          format(self.dummy_package))
        self.dummy_rpm_local_path = ('{0}/plugins/{1}'.
                            format(os.path.dirname(__file__), self.dummy_rpm))

        self.cluster2 = {'id': 'c2',
                         'cluster_id': '1043',
                         'script': 'expand_cloud_c2_mn2.sh',
                         'node': 'node2'}
        self.cluster3 = {'id': 'c3',
                         'cluster_id': '1044',
                         'script': 'expand_cloud_c3_mn3.sh',
                         'node': 'node3'}
        self.cluster4 = {'id': 'c4',
                         'cluster_id': '1045',
                         'script': 'expand_cloud_c4_mn4.sh',
                         'node': 'node4'}

    def tearDown(self):
        """Runs for every test"""
        super(Story11872, self).tearDown()

    def _make_sure_dummy_rpm_is_available_locally(self):
        """
            Description check for the existence of dumt RPM file locally
        """
        cmd = "[ -f {0} ]".format(self.dummy_rpm_local_path)
        _, _, rc = self.run_command_local(cmd)
        self.assertEqual(0, rc,
            'File "{0}" was not found on local machine'.
            format(self.dummy_rpm_local_path))

    def _create_cluster(self, cluster_collect, cluster_id, unique_id, script):
        """
        Description
            Run the expansion procedure to obtain a 3 clusters 1 node
            configuration
        Args:
            cluster_collect (str) : cluster collection item
            cluster_id (str)      : cluster id (item path suffix)
            unique_id (str)       : cluster unique identifier
            script (str)          : expand script
        """
        self.log('info',
        'EXPANSION: Create a new cluster "{0}"'.format(cluster_id))

        props = (
        'cluster_type=sfha low_prio_net=mgmt llt_nets=hb1,hb2 cluster_id={0}'.
        format(unique_id))

        path = '{0}/{1}'.format(cluster_collect, cluster_id)
        self.execute_cli_create_cmd(self.ms1, path, 'vcs-cluster', props,
                                    add_to_cleanup=False)

        self.log('info',
        'EXPANSION: Execute the expand script for cluster {0} using "{1}" '.
        format(cluster_id, script))
        self.execute_expand_script(self.ms1, script)

        return path

    def _run_restore_snapshot_and_wait_for_fail(self, args=''):
        """
        Description:
            Make sure dummy plugin is installed so that restore snapshot
            fails at phase1
        Args:
            args (str): Argument for the restore_snapshot command
        """
        self._install_dummy_package()
        self.execute_cli_restoresnapshot_cmd(self.ms1, args=args)
        plan = self.execute_cli_showplan_cmd(self.ms1)[0]
        self.assertTrue(self.wait_for_plan_state(self.ms1,
                                                test_constants.PLAN_FAILED,
                                                timeout_mins=1))
        return plan

    def _configure_cluster(self, cluster, cluster_collect_url):
        """
        Description:
            Check if specified cluster is in the model.
            Create one if cluster is not present
        Args:
            cluster (dict): cluster data
            cluster_collect_url (str): The cluster collection item on model
        """
        cluster_urls = sorted(self.find(self.ms1,
                                        '/deployments', 'vcs-cluster'))
        # Check if cluster exists on model
        expanded = False
        url = None
        for cluster_url in cluster_urls:
            if re.search('{0}$'.format(cluster['id']), cluster_url):
                url = cluster_url
                break
        # We did not find it so let's create it
        if url == None:
            self._create_cluster(cluster_collect_url,
                    cluster['id'], cluster['cluster_id'], cluster['script'])
            expanded = True

        return expanded

    def _get_task_url(self, plan_output, desc, log=True):
        """
        Description:
            Parse the restore snapshot plan to get the list of task target item
            whose task description matches the given string
        Args:
            plan_output (list) : Plan output
            desc (str) : String to look for on task description
            log (bool) : Specify wheter to dump log or not
        """
        plan_dict = self.cli.parse_plan_output(plan_output)
        task_urls = []
        for phase_index, phase in sorted(plan_dict.iteritems()):
            for task_index, task in sorted(phase.iteritems()):
                if len([x for x in task['DESC'] if desc in x]) > 0:
                    if log:
                        self.log('info', 'Phase {0} - Task {1}'.
                                 format(phase_index, task_index))
                        for description in task['DESC']:
                            self.log('info', description)

                    if '/ms' not in task['DESC'][0]:
                        task_urls.append(task['DESC'][0])
        return task_urls

    def _run_deployment_expansion_plan(self, nodes_to_expand):
        """
        Description:
            Run the deployment expansion plan and set password on nodes
        Args:
            nodes_to_expand (list) : Nodes to be added
        """
        self.run_and_check_plan(self.ms1,
                    test_constants.PLAN_COMPLETE, 60, add_to_cleanup=False)

        for node in nodes_to_expand:
            self.assertTrue(self.set_pws_new_node(self.ms1, node),
                    'Failed to set password on "{0}'.format(node))

    def _verify_restart_sequence(self, expected_seq, actual_seq):
        """
        Description:
            Assert that actual node restart sequence matches the expected
            sequence
        Args:
            expected_tasks (list) : Expected restart task
            actual_tasks (list)   : Actual restart tasks
        """
        self.log('info',
            'Expected restart sequence: {0}'.
            format(', '.join(expected_seq)))
        self.log('info',
            'Actual restart sequence:   {0}'.
            format(', '.join(actual_seq)))
        self.assertEqual(expected_seq, actual_seq,
            '\nExpected restart sequence NOT found'
            '\nExpected restart sequence\n{0}'
            '\nActual restart sequence\n{1}'.
            format('\n'.join(expected_seq), '\n'.join(actual_seq)))

    def _install_dummy_package(self):
        """
        Description:
            Install dummy package into MS
            This package will cause each restore_snapshot plan to fail
            at phase1
        """
        if self.check_pkgs_installed(self.ms1, [self.dummy_package]) is False:
            self.copy_and_install_rpms(self.ms1, [self.dummy_rpm_local_path])
            self.assertTrue(
                self.check_pkgs_installed(self.ms1, [self.dummy_package]),
                'Failed to install package {0}'.format(self.dummy_package))

    def _remove_dummy_package(self):
        """
        Description:
            Remove dummy package from MS
            Removing this package allows restore_snapshot plan to run to
            completion
        """
        if self.check_pkgs_installed(self.ms1, [self.dummy_package]) is True:
            cmd = self.rhcmd.get_yum_remove_cmd([self.dummy_package])
            self.run_command(self.ms1, cmd, su_root=True,
                            default_asserts=True, add_to_cleanup=False)

            self.assertFalse(
                self.check_pkgs_installed(self.ms1, [self.dummy_package]),
                'Failed to remove package {0}'.format(self.dummy_package))

    def _expand_deployment(self, clusters_to_add):
        """
        Description:
            Add cluster to LITP model and run the plan
        Args:
            number_of_nodes (int) : the number of nodes to expand to
        """
        cluster_collect_url = self.find(self.ms1, '/deployments',
                                        'cluster', False)[0]
        expanded = False
        nodes_to_expand = []
        for cluster in clusters_to_add:
            expanded = self._configure_cluster(cluster, cluster_collect_url)
            if expanded:
                nodes_to_expand.append(cluster['node'])

        if len(nodes_to_expand) != 0:
            self.log('info',
            'Run plan and wait for it to complete the expansion')
            self._run_deployment_expansion_plan(nodes_to_expand)

    def _delete_dependency_list(self, clusters):
        """
        Description:
            Delete property "dependecy_list" from each cluster on list
        Args:
            clusters (list): list of cluster item urls
        """
        for cluster in clusters:
            props = self.get_props_from_url(self.ms1, cluster)
            if props.get('dependency_list'):
                self.execute_cli_update_cmd(self.ms1, cluster,
                                            'dependency_list',
                                            action_del=True)

    def _update_dependency_list(self, property_list, cluster_list):
        """
        Updates the dependency list

        Parameters:

        property_list: tuple of (cluster_url, dependency list value)
        cluster_list: cluster_url whose properties should be printed
        """
        for url, cluster in property_list:
            self.execute_cli_update_cmd(self.ms1, url,
                    "dependency_list={0}".format(cluster))

        for url in cluster_list:
            self.get_props_from_url(self.ms1, url)

    def _expand_cluster(self, clusters_to_add):
        """
        Expands the cluster
        """
        self._make_sure_dummy_rpm_is_available_locally()
        self.remove_all_snapshots(self.ms1)
        self._expand_deployment(clusters_to_add)

        cluster_urls = sorted(self.find(self.ms1,
            '/deployments', 'vcs-cluster'))
        self._delete_dependency_list(cluster_urls)
        return cluster_urls

    def _restore_model_expect_sequence(self, expected_sequence):
        """
        Runs restore snapshot and asserts the order of cluster restarts
        """

        for url in expected_sequence:
            self.get_props_from_url(self.ms1, url)

        plan_output = self._run_restore_snapshot_and_wait_for_fail()
        actual_sequence = self._get_task_url(plan_output, 'Restart')
        self.execute_cli_restoremodel_cmd(self.ms1)

        self.log('info',
        'Verify that nodes restarted according to the dependency graph')
        self._verify_restart_sequence(expected_sequence, actual_sequence)

    @attr('all', 'expansion', 'non-revert', 'story11872', 'story11872_tc01')
    def test_01_p_dependency_graph_with_deployment_expansion(self):
        """
        @tms_id: litpcds_11872_tc01
        @tms_requirements_id: LITPCDS-11872
        @tms_title: Dependency graph with deployment expansion
        @tms_description:
            Verify that a user can restore a snapshot created with
            depenecy_list set on clusters to shrink back an
            expanded deployment
            NOTE: also verifies LITPCDS-13469
        @tms_test_steps:
            @step: Configure a 2 clusters environment
            @result: 2nd cluster exists in the model
            @step: Set property "dependency_list" on clusters items so that
                  C1-->C2
            @result: Dependency list is updated
            @step: Create deployment snapshot
            @result: Snapshot is created
            @step: Expand cluster to 3 nodes configuration
            @result: i3rd node is in the cluster
            @step: Set property "dependency_list" on clusters items so that
                C1-->C2 and C2-->C3
            @result: Dependency list is updated
            @step: Restore snapshot, capture plan tasks and stop plan
                 immediately
            @result: restore_snapshot fails
            @step:  Run Restore_snapshot
            @result: system is back in a 2 nodes state
            @result: Nodes restarted in according to the dependency graph
            @result: No VCS task for cluster c3 are present on the
               restore_snapshot plan (LITPCDS-13469)

        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info',
        'Configure a 2 clusters environment')
        c1_url, c2_url = self._expand_cluster(
                [self.cluster2])

        self.log('info',
        'Set property "dependency_list" on clusters items so that C1-->C2')

        self._update_dependency_list(
                [[c1_url, "c2"]],
                [c1_url, c2_url])

        self.log('info',
        'Create deployment snapshot')
        self.execute_and_wait_createsnapshot(self.ms1, add_to_cleanup=False)

        self.log('info',
        'Expand cluster to 3 nodes configuration')
        clusters_to_add = (self.cluster2, self.cluster3)
        self._expand_deployment(clusters_to_add)

        c3_url = sorted(self.find(self.ms1, '/deployments', 'vcs-cluster'))[2]

        self.log('info',
        'Set property "dependency_list" on clusters items so that '
            'C1-->C2 and C2-->C3')
        self._update_dependency_list([[c2_url, "c3"]],
                [c1_url, c2_url, c3_url])

        self.log('info',
        'Restore snapshot, capture plan tasks and stop plan immediately')
        plan_output = self._run_restore_snapshot_and_wait_for_fail()
        actual_sequence = self._get_task_url(plan_output, 'Restart')

        self.log('info',
        'Capture VCS tasks')
        vcs_tasks_itmes = self._get_task_url(plan_output, 'VCS')

        self.log('info',
        'Restore_snapshot to bring us back to a 2 nodes state')
        self.execute_cli_removeplan_cmd(self.ms1)
        self._remove_dummy_package()
        self.restart_litpd_service(self.ms1)
        self.execute_and_wait_restore_snapshot(self.ms1,
                    poweroff_nodes=[self.cluster3['node']], timeout_mins=60)

        self.log('info',
        'Verify that nodes restarted according to the dependency graph')
        expected_sequence = [c2_url, c1_url]
        self._verify_restart_sequence(expected_sequence, actual_sequence)

        self.log('info',
        'Verify that NO VCS task for cluster c3 were on the '
            'restore_snapshot plan (LITPCDS-13469)')
        for url in vcs_tasks_itmes:
            self.assertNotEqual(c3_url, url,
            'VCS tasks for cluster "{0}" was found on restore_snapshot plan'.
            format(c3_url))

    @attr('all', 'expansion', 'non-revert', 'story11872', 'story11872_tc10')
    def test_10_p_shutting_down_nodes_before_restore_snapshot(self):
        """
        @tms_id: litpcds_11872_tc10
        @tms_requirements_id: LITPCDS-11872
        @tms_title: Dependency graph - shut down nodes before restore
        @tms_description:
            Verify that when a user run "restore_snapshot" and clusters
            have property "dependency_list" set, if all nodes on one cluster
            are off-line at time of restore_snapshot, then nodes restart tasks
            are sequenced according to dependency graph.
        @tms_test_steps:
            @step: Configure a 3 clusters environment
            @result: 3rd cluster exists in the model
            @step: Set property "dependency_list" on clusters items so that
               C1-->C2, C2-->C3
            @result: Dependency list is updated
            @step: Create deployment snapshot
            @result: Snapshot is created
            @step: Power off node2
            @result: Node is powered off
            @step: Restore snapshot, capture plan tasks and stop plan
                 immediately
            @result: restore_snapshot fails
            @result: Nodes restarted in according to the dependency graph

        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Configure a 3 clusters environment')
        c1_url, c2_url, c3_url = self._expand_cluster(
                [self.cluster2, self.cluster3])

        self.log('info',
        'Set property "dependency_list" on clusters items so that '
            'C1-->C2, C2-->C3')
        self._update_dependency_list(
                ((c1_url, "c2"),
                 (c2_url, "c3")
                 ),
                [c1_url, c2_url, c3_url])

        self.log('info',
        'Create deployment snapshot')
        self.execute_and_wait_createsnapshot(self.ms1, add_to_cleanup=False)

        self.log('info',
        'Power off node2')
        self.poweroff_peer_node(self.ms1, self.cluster2['node'])

        self.log('info',
        'Restore snapshot, capture plan tasks and stop plan immediately')
        self._restore_model_expect_sequence([c3_url, c2_url, c1_url])

        self.log('info',
        'Power on node2')
        self.poweron_peer_node(self.ms1, self.cluster2['node'])

    @attr('pre-reg', 'expansion', 'non-revert', 'story11872',
          'story11872_tc11')
    def test_11_p_valid_interdependant_clusters_dependency_graph(self):
        """
        @tms_id: litpcds_11872_tc11
        @tms_requirements_id: LITPCDS-11872
        @tms_title: Dependency graph - validate independent connected
            components
        @tms_description:
            Verify that when users run "restore_snapshot" and clusters
            have property "dependency_list" set, clusters are
            restarted in the sequence according to dependency graph.
        @tms_test_steps:
            @step: Configure a 4 clusters environment
            @result: 4 clusters exists in the model
            @step: Set property "dependency_list" on clusters items
            so that
               C1-->C2, C3-->C4
            @result: Dependency list is updated
            @step: Create deployment snapshot
            @result: Snapshot is created
            @step: Restore snapshot
            @result: Verify C2 comes online before C1 and C4 comes
            online before C3

        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        self.log('info', 'Configure a 4 clusters environment')
        c1_url, c2_url, c3_url, c4_url = self._expand_cluster(
            [self.cluster2, self.cluster3, self.cluster4])

        self.log('info',
                 'Set property "dependency_list" on clusters items so that '
                 'C2-->C1, C4-->C3')
        self._update_dependency_list(
            ((c1_url, "c2"),
             (c3_url, "c4")
             ),
            [c1_url, c2_url, c3_url, c4_url])

        self.log('info',
                 'Create deployment snapshot')
        self.execute_and_wait_createsnapshot(self.ms1, add_to_cleanup=False)

        self.log('info', 'Restore snapshot,'
                         ' capture plan tasks and stop plan immediately')
        self._restore_model_expect_sequence([c2_url, c4_url, c1_url, c3_url])

    @attr('pre-reg', 'expansion', 'non-revert', 'story11872',
          'story11872_tc12')
    def test_12_p_invalid_interdependant_clusters_dependency_graph(self):
        """
        @tms_id: litpcds_11872_tc12
        @tms_requirements_id: LITPCDS-11872
        @tms_title: Dependency graph - invalidate independent connected
            components
        @tms_description:
            Verify that when users run "restore_snapshot" and clusters
            have an invalid "dependency_list" set, clusters are
            restarted in an non re-ordered sequence.

        @tms_test_steps:
            @step: Configure a 4 clusters environment
            @result: 4 clusters exists in the model
            @step: Set property "dependency_list" on clusters items
            so that
               C2-->C1, C3-->C4, C4-->C3
            @result: Dependency list is updated
            @step: Create deployment snapshot
            @result: Snapshot is created
            @step: Restore snapshot
            @result: Verify that clusets come online in non re-ordered
            sequence.

        @tms_test_precondition: NA
        @tms_execution_type: Automated
        """
        start_log = self.get_file_len(self.ms1,
                                      test_constants.GEN_SYSTEM_LOG_PATH)

        self.log('info', 'Configure a 3 clusters environment')
        c1_url, c2_url, c3_url, c4_url = self._expand_cluster(
            [self.cluster2, self.cluster3, self.cluster4])

        self.log('info',
                 'Set property "dependency_list" on clusters items so that '
                 'C1-->C2, C3-->C4, C4-->C3')
        self._update_dependency_list(
            ((c1_url, "c2"),
             (c3_url, "c4"),
             (c4_url, "c3")
             ),
            [c1_url, c2_url, c3_url, c4_url])

        self.log('info',
                 'Create deployment snapshot')
        self.execute_and_wait_createsnapshot(self.ms1, add_to_cleanup=False)

        self.assertTrue(self.wait_for_log_msg(self.ms1,
                        "WARNING: Order of clusters is invalid.",
                        log_len=start_log), "Log not found")

        self.log('info', 'Restore snapshot,'
                         ' capture plan tasks and stop plan immediately')
        self._restore_model_expect_sequence([c1_url, c2_url, c3_url, c4_url])

        self.assertTrue(self.wait_for_log_msg(self.ms1,
                        "WARNING: Order of clusters is invalid.",
                        log_len=start_log), "Log not found")
