package org.easyjob.allocation;

import lombok.extern.slf4j.Slf4j;
import org.easyjob.JobDefinition;
import org.easyjob.allocation.sharding.ShardingService;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.JobShardDefinition;
import org.easyjob.membership.Membership;
import org.easyjob.repository.DefinitionChangedListener;

import java.util.List;

/**
 * 该类是基于Event Driven 的方式进行快速响应,减少任务分配工作延迟
 * todo 另外还有一个全量同步机制，每5分钟运行一次, 防止Event丢失导致有job没有被分配
 */
@Slf4j
public class AllocationRepositoryChangedListener implements DefinitionChangedListener {

    private AllocationTable allocationTable;
    private AllocationService allocationService;
    private Membership membership;

    public AllocationRepositoryChangedListener(AllocationTable allocationTable, AllocationService allocationService, Membership membership) {
        this.allocationTable = allocationTable;
        this.allocationService = allocationService;
        this.membership = membership;
    }

    //todo 节点启动后，repo 里的每个已存在job都会重新触发一次create事件，导致重复提交任务
    @Override
    public void created(JobDefinition jobDefinition) {
        if (!membership.hasLeadership()) {
            return;
        }
        if (allocationTable.containJob(jobDefinition.getId())) {
            return;
        }
        log.info("aware of the 'create' event.jobId is:{}", jobDefinition.getId());
        List<JobShardDefinition> jobShardInfos = ShardingService.generateShards(jobDefinition);
        jobShardInfos.forEach(shard -> allocationTable.put(allocationService.allocateShard(shard), shard));
    }

    @Override
    public void removed(JobDefinition jobDefinition) {
        if (!membership.hasLeadership()) {
            return;
        }
        log.info("aware of the 'removed' event.jobId is:{}", jobDefinition.getId());
        allocationTable.removeByJobId(jobDefinition.getId());
    }

    @Override
    public void updated(JobDefinition jobDefinition) {
        if (!membership.hasLeadership()) {
            return;
        }
        log.info("aware of the 'updated' event.jobId is:{}", jobDefinition.getId());

        this.removed(jobDefinition);
        this.created(jobDefinition);
    }
}
