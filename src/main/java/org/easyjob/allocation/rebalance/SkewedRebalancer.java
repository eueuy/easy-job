package org.easyjob.allocation.rebalance;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.easyjob.allocation.AllocationService;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.JobShardDefinition;
import org.easyjob.allocation.table.NodeCapacityInfo;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Slf4j
//todo 算法要重新考虑了， 现在并不会主动将shard分散开来。
public class SkewedRebalancer implements Rebalancer {

    private AllocationService allocationService;
    private AllocationTable allocationTable;

    public SkewedRebalancer(AllocationService allocationService, AllocationTable allocationTable) {
        this.allocationService = allocationService;
        this.allocationTable = allocationTable;
    }

    @Override
    public void tryRebalance() {
        List<NodeCapacityInfo> nodeCapacityInfos = allocationTable.fetchNodeCapacityInfos();
        if (nodeCapacityInfos.isEmpty()) {
            return;
        }

        double avgUsageRate = computeAvgUsageRate(nodeCapacityInfos);

        if (isNeedRebalance(nodeCapacityInfos, avgUsageRate)) {

            NodeCapacityInfo busiestNode = getBusiestNode(nodeCapacityInfos);
            int howManyJobsNeedBeMoveOut = computeHowManyJobsNeedBeMoveOut(busiestNode, avgUsageRate);
            log.info("<skewed rebalancer> will reallocate out " + howManyJobsNeedBeMoveOut + " jobs from node<" + busiestNode.getId() + ">");

            reallocate(busiestNode, howManyJobsNeedBeMoveOut);
            log.info("<skewed rebalancer> rebalance finished. nodes capacity is : " + JSON.toJSONString(nodeCapacityInfos));
        } else {
            log.info("<skewed rebalancer> no need to rebalance . new nodes capacity is : " + JSON.toJSONString(allocationTable.fetchNodeCapacityInfos()));
        }
    }

    private void reallocate(NodeCapacityInfo busiestNode, int count) {
        List<JobShardDefinition> shardsFromBusiestNode = allocationTable.listShardsByNode(busiestNode.getId());

        for (int moved = 0; moved < count && !shardsFromBusiestNode.isEmpty(); ) {
            //todo 这里不能随便拿个任务就去rebalance了， 要优先拿有拥挤的job
            JobShardDefinition shardNeedBeMove = shardsFromBusiestNode.get(0);
            String targetNodeId = allocationService.allocateShard(shardNeedBeMove);

            if (!busiestNode.getId().equals(targetNodeId)) {
                allocationTable.move(shardNeedBeMove, busiestNode.getId(), targetNodeId);
                moved++;
            }
            shardsFromBusiestNode.remove(0);
        }
    }


    private int computeHowManyJobsNeedBeMoveOut(NodeCapacityInfo node, double avgUsageRate) {
        double busiestNodeUsageRateStd = node.getUsageRateStd(avgUsageRate);
        return (int) (double) (node.getUsedCapacity() * busiestNodeUsageRateStd);
    }

    private NodeCapacityInfo getBusiestNode(List<NodeCapacityInfo> nodeCapacityInfos) {
        return Collections.max(nodeCapacityInfos, Comparator.comparingDouble(NodeCapacityInfo::getUsageRate));
    }


    private boolean isNeedRebalance(List<NodeCapacityInfo> nodeCapacityInfos, double avgUsageRate) {
        for (NodeCapacityInfo nodeCapacityInfo : nodeCapacityInfos) {
            double usageRateStd = nodeCapacityInfo.getUsageRateStd(avgUsageRate);
            if ((Math.abs(usageRateStd) > 0.25)) {
                return true;
            }
        }
        return false;
    }

    private double computeAvgUsageRate(List<NodeCapacityInfo> nodeCapacityInfos) {
        return nodeCapacityInfos.stream().mapToDouble(NodeCapacityInfo::getUsageRate).average().orElse(Double.NaN);
    }

}
