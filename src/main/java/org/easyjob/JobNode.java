package org.easyjob;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.easyjob.allocation.*;
import org.easyjob.allocation.rebalance.RebalanceTrigger;
import org.easyjob.allocation.rebalance.SkewedRebalancer;
import org.easyjob.allocation.rebalance.UnassignedRebalancer;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.ZookeeperAllocationTable;
import org.easyjob.execution.CancelAllJobConnectionLostListener;
import org.easyjob.execution.DefaultExecutionMachine;
import org.easyjob.execution.ExecutionMachine;
import org.easyjob.execution.NodeExecutionManager;
import org.easyjob.membership.LeaderChangedListener;
import org.easyjob.membership.Membership;
import org.easyjob.membership.MembershipChangedListener;
import org.easyjob.repository.DefinitionChangedListener;
import org.easyjob.repository.DefinitionRepository;
import org.easyjob.repository.ZookeeperDefinitionRepository;
import org.easyjob.util.UUIDUtil;
import org.easyjob.zookeeper.ConnectionLostListener;
import org.easyjob.zookeeper.CuratorClientHelper;
import org.easyjob.zookeeper.ZookeeperConfiguration;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

@SuppressWarnings({"WeakerAccess", "unused"})
@Slf4j
public class JobNode {

    private NodeConfiguration nodeConfiguration;
    private CuratorClientHelper zkClientHelper;
    private Membership membership;
    private RebalanceTrigger rebalanceTrigger;
    private ExecutionMachine executionMachine;
    private NodeExecutionManager executionManager;
    private DefinitionRepository definitionRepository;
    private JobService jobService;

    public JobNode(String clusterName, String nodeId, int maxCapacity, int threadSize, ZookeeperConfiguration zookeeperConfiguration) {
        this(NodeConfiguration.builder()
                        .clusterName(clusterName)
                        .nodeId(nodeId)
                        .maxCapacity(maxCapacity)
                        .threadSize(threadSize)
                        .build(),
                zookeeperConfiguration);
    }


    public JobNode(String clusterName, int maxCapacity, int threadSize, ZookeeperConfiguration zookeeperConfiguration) {
        this(clusterName, DefaultNodeIdGenerator.getId(), maxCapacity, threadSize, zookeeperConfiguration);
    }

    public JobNode(String clusterName, ZookeeperConfiguration zookeeperConfiguration) {
        this(NodeConfiguration.builder()
                        .clusterName(clusterName)
                        .build(),
                zookeeperConfiguration);
    }

    public JobNode(ZookeeperConfiguration zookeeperConfiguration) {
        this(NodeConfiguration.builder().build(), zookeeperConfiguration);
    }

    public JobNode(String zookeeperConnectString) {
        this(NodeConfiguration.builder().build(),
                ZookeeperConfiguration.builder().connectString(zookeeperConnectString).build());
    }

    public JobNode(NodeConfiguration nodeConfiguration, ZookeeperConfiguration zookeeperConfiguration) {
        Objects.requireNonNull(nodeConfiguration);
        Objects.requireNonNull(zookeeperConfiguration);
        this.nodeConfiguration = nodeConfiguration;

        try {

            //start execution machine ,ready to accept job
            this.executionMachine = new DefaultExecutionMachine(nodeConfiguration.getThreadSize());

            //open zookeeper connection
            this.zkClientHelper = new CuratorClientHelper(nodeConfiguration.getClusterName(), zookeeperConfiguration);
            CuratorFramework client = zkClientHelper.getClient();
            this.zkClientHelper.addConnectionLostListener(new CancelAllJobConnectionLostListener(executionMachine));

            //connect to job registry
            AllocationTable allocationTable = new ZookeeperAllocationTable(client);
            allocationTable.addNode(nodeConfiguration.getNodeId(), nodeConfiguration.getMaxCapacity());

            //join to cluster
            this.membership = new Membership(client);
            this.membership.addLeaderChangedListener(new ApplyAvailableLeaderChangedListener(allocationTable));
            this.membership.addMembershipChangedListener(new ApplyAvailableMembershipChangedListener(allocationTable));
            this.membership.addMembershipChangedListener(new NodeRejoinMembershipChangedListener(nodeConfiguration, allocationTable, executionMachine));
            this.membership.join(nodeConfiguration.getNodeId());

            //create job allocator service
            AllocationService allocationService = new AllocationService(allocationTable);

            this.definitionRepository = new ZookeeperDefinitionRepository(client);
            this.definitionRepository.addListener(new AllocationRepositoryChangedListener(allocationTable, allocationService, membership));

            //create job service
            this.jobService = new JobService(definitionRepository);

            //start job puller , pull job from job-registry to execution-machine
            this.executionManager = new NodeExecutionManager(membership.getNodeId(), allocationTable, executionMachine, definitionRepository);
            this.executionManager.start();

            //switch rebalancer to ready (wait to takes leadership to execute rebalance)
            this.rebalanceTrigger = new RebalanceTrigger(membership);
            this.rebalanceTrigger.addRebalancer(new UnassignedRebalancer(allocationService, allocationTable));
            this.rebalanceTrigger.addRebalancer(new SkewedRebalancer(allocationService, allocationTable));
            this.rebalanceTrigger.start();

            log.info("job node start successful");
        } catch (Exception e) {
            log.error("job node start failure, cause: " + e.getMessage(), e);
            this.shutdown();
            throw new IllegalStateException(e);
        }
    }

    public void shutdown() {
        if (rebalanceTrigger != null) {
            rebalanceTrigger.close();
        }
        if (executionManager != null) {
            executionManager.close();
        }
        if (executionMachine != null) {
            executionMachine.shutdown();
        }
        if (membership != null) {
            membership.close();
        }
        if (definitionRepository != null) {
            definitionRepository.close();
        }
        if (zkClientHelper != null) {
            zkClientHelper.close();
        }
        log.info("job node <{}> has shutdown", nodeConfiguration.getNodeId());
    }

    public void addMembershipChangedListener(MembershipChangedListener listener) {
        this.membership.addMembershipChangedListener(listener);
    }

    public void addConnectionLostListener(ConnectionLostListener listener) {
        this.zkClientHelper.addConnectionLostListener(listener);
    }

    public void addLeaderChangedListener(LeaderChangedListener listener) {
        this.membership.addLeaderChangedListener(listener);
    }

    public void addJobDefinitionChangedListener(DefinitionChangedListener listener) {
        this.definitionRepository.addListener(listener);
    }

    public String getLeaderId() {
        return this.membership.getLeaderId();
    }

    public boolean isLeader() {
        return this.membership.hasLeadership();
    }


    public JobService getJobService() {
        return this.jobService;
    }


    @SuppressWarnings("WeakerAccess")
    @Slf4j
    public static class DefaultNodeIdGenerator {
        public static String getId() {
            try {
                InetAddress localHost = Inet4Address.getLocalHost();
                return localHost.getHostAddress();
            } catch (UnknownHostException e) {
                log.warn("can not getJobInstance host address for node id, use UUID instead, cause :" + e.getMessage(), e);
                return UUIDUtil.generate8Code();
            }
        }

    }


}
