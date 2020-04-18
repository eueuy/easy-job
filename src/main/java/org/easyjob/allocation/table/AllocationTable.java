package org.easyjob.allocation.table;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public interface AllocationTable {

    //used by allocation

    void put(String nodeId, JobShardDefinition shard);

    @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos();

    void removeByJobId(String jobId);

    void remove(JobShardDefinition shard);

    boolean containJob(String jobId);

    boolean contain(JobShardDefinition shard);

    Map<String, Integer> fetchJobsAllocationTable(String jobId);

    //used by rebalancer

    @NotNull List<JobShardDefinition> listUnassignedShards();

    @Nullable
    String locateNodeByShard(JobShardDefinition shard);

    void move(JobShardDefinition shard, String sourceNode, String targetNode);

    //used by node execution manager

    @NotNull List<JobShardDefinition> listShardsByNode(String nodeId);

    //used by membership listener
    void applyAvailableOfNodes(List<String> availableNodes);

    //used by node init
    void addNode(String nodeId, int maxCapacity);


}
