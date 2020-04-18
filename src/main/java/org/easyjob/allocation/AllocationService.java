package org.easyjob.allocation;

import lombok.extern.slf4j.Slf4j;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.JobShardDefinition;
import org.easyjob.allocation.table.NodeCapacityInfo;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class AllocationService {

    private AllocationTable allocationTable;

    public AllocationService(AllocationTable allocationTable) {
        this.allocationTable = allocationTable;
    }

    /**
     * input a job , return a map<target-node,shard-number-of-job>
     */
    @NotNull
    public String allocateShard(JobShardDefinition shard) {
        List<NodeCapacityInfo> nodeCapacityInfos = allocationTable.fetchNodeCapacityInfos();

        //选出有空间的node ,如果有多个，进入下一轮 ，如果没有，中断，如果只有一个，获胜
        List<NodeCapacityInfo> capacityFreeNodes = selectCapacityFreeNodes(nodeCapacityInfos);

        if (capacityFreeNodes.isEmpty()) {
            throw new IllegalStateException("no free capacity to allocate shard : " + shard);
        }
        if (capacityFreeNodes.size() == 1) {
            return capacityFreeNodes.get(0).getId();
        }


        //选出冲突最小的node ，如果有多个并列，进入下一轮，如果只有一个，获胜 ，不可能一个都没有
        List<NodeCapacityInfo> conflictLessNodes = selectConflictLessNodes(shard, capacityFreeNodes);

        if (conflictLessNodes.isEmpty()) {
            throw new IllegalStateException("no conflict less node find when allocate shard : " + shard);
        }
        if (conflictLessNodes.size() == 1) {
            return conflictLessNodes.get(0).getId();
        }


        //选出空闲度最大的node
        NodeCapacityInfo maxCapacityNode = selectMaxCapacityNode(conflictLessNodes);
        String targetNode = maxCapacityNode.getId();
        log.info("shard <{}> 's target node is: {} ", shard, targetNode);
        return targetNode;
    }

    @NotNull
    private List<NodeCapacityInfo> selectCapacityFreeNodes(List<NodeCapacityInfo> nodeCapacityInfos) {
        return nodeCapacityInfos.stream().filter(node -> node.getFreeCapacity() >= 1).collect(Collectors.toList());
    }

    @NotNull
    private List<NodeCapacityInfo> selectConflictLessNodes(JobShardDefinition shard, List<NodeCapacityInfo> nodes) {
        Map<String, Integer> shardCount = allocationTable.fetchJobsAllocationTable(shard.getJobId());

        try {
            String minConflictNode = nodes.stream().min(Comparator.comparing(o -> shardCount.getOrDefault(o.getId(), 0))).orElseThrow((Supplier<Throwable>) () -> new IllegalStateException("can not got min crowd node")).getId();
            Integer minConflictNodeCount = shardCount.getOrDefault(minConflictNode, 0);
            return nodes.stream().filter(
                    node -> shardCount.getOrDefault(node.getId(), 0).equals(minConflictNodeCount))
                    .collect(Collectors.toList());

        } catch (Throwable throwable) {
            throw new IllegalStateException("cannot select conflict less node ,cause: " + throwable.getMessage(), throwable);
        }
    }

    private NodeCapacityInfo selectMaxCapacityNode(List<NodeCapacityInfo> nodeCapacityInfos) {
        nodeCapacityInfos.sort(Comparator.comparingDouble(NodeCapacityInfo::getUsageRate));
        return nodeCapacityInfos.get(0);
    }

}
