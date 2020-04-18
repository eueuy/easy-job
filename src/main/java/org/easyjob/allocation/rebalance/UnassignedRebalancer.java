package org.easyjob.allocation.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.easyjob.allocation.AllocationService;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.JobShardDefinition;

import java.util.List;

/**
 * try rebalance jobs of unassigned-node to another nodes in cluster.
 */
@Slf4j
public class UnassignedRebalancer implements Rebalancer {

    private AllocationService allocationService;
    private AllocationTable allocationTable;

    public UnassignedRebalancer(AllocationService allocationService, AllocationTable allocationTable) {
        this.allocationService = allocationService;
        this.allocationTable = allocationTable;
    }


    @Override
    public void tryRebalance() {
        List<JobShardDefinition> unassignedShards = allocationTable.listUnassignedShards();
        if (unassignedShards.isEmpty()) {
            log.info("<unassigned rebalancer> no need to rebalance ");
        } else {
            rebalanceUnassignedShards(unassignedShards);
        }
    }

    private void rebalanceUnassignedShards(List<JobShardDefinition> unassignedShards) {
        log.info("<unassigned rebalancer> starting to rebalance " + unassignedShards.size() + " jobs");
        unassignedShards.forEach(shard -> {
            try {
                String targetNode = allocationService.allocateShard(shard);
                String sourceNode = allocationTable.locateNodeByShard(shard);
                if (sourceNode != null) {
                    allocationTable.move(shard, sourceNode, targetNode);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        log.info("<unassigned rebalancer> rebalance finished");
    }

}
