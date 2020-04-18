package org.easyjob.execution;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.easyjob.Job;
import org.easyjob.JobDefinition;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.JobShardDefinition;
import org.easyjob.repository.DefinitionRepository;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job拉取器 ： 用来拉取属于本节点的任务并提交到执行机
 */
@Slf4j
public class NodeExecutionManager {

    private AllocationTable allocationTable;
    private DefinitionRepository definitionRepository;
    private String nodeId;
    private ExecutionMachine executionMachine;
    private ScheduledFuture<?> scheduledFuture;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("job-puller-%d").build());

    public NodeExecutionManager(String nodeId, AllocationTable allocationTable, ExecutionMachine executionMachine, DefinitionRepository definitionRepository) {
        this.allocationTable = allocationTable;
        this.executionMachine = executionMachine;
        this.nodeId = nodeId;
        this.definitionRepository = definitionRepository;
    }

    public void start() {
        scheduledFuture = executor.scheduleWithFixedDelay(this::syncShardsToMachine, 5, 60, TimeUnit.SECONDS);
    }

    public void close() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void syncShardsToMachine() {
        try {
            List<JobShardDefinition> myShardsInAllocationTable = allocationTable.listShardsByNode(nodeId);
            int removeCount = processRemovedOrOutdatedShards(myShardsInAllocationTable);
            int addingCount = processDiscoveredShards(myShardsInAllocationTable);
            log.info("node<{}> has total of {} jobs in the registry. {} were deleted and {} were added in this round",
                    nodeId,
                    myShardsInAllocationTable.size(),
                    removeCount,
                    addingCount);

        } catch (Exception e) {
            log.error("node <" + nodeId + "> cannot pull job from registry, cause: " + e.getMessage(), e);
        }
    }

    private int processDiscoveredShards(List<JobShardDefinition> myShardsInAllocationTable) {
        AtomicInteger counter = new AtomicInteger(0);
        myShardsInAllocationTable.forEach(shard -> {
            try {
                if (!executionMachine.exist(shard)) {
                    JobDefinition jobDefinition = this.definitionRepository.get(shard.getJobId());
                    if (jobDefinition.isEnable()) {
                        log.info("there is a new job <{}> found in the registry", shard);
                        executionMachine.execute(shard, jobDefinition);
                        counter.getAndIncrement();
                    }
                }
            } catch (Exception e) {
                log.error("shard <" + shard + "> execute to execution machine failure , cause:" + e.getMessage(), e);
            }
        });
        return counter.get();
    }

    private int processRemovedOrOutdatedShards(List<JobShardDefinition> myShardsInAllocationTable) {
        AtomicInteger counter = new AtomicInteger(0);
        for (JobShardDefinition shard : executionMachine.list()) {
            try {
                if (shard == null) {
                    log.error("execution machine list has ‘null’ element");
                    continue;
                }
                if (myShardsInAllocationTable.contains(shard)) {
                    if (isVersionOutdated(shard)) {
                        log.info("there is a new version of shard <{}> in the registry", shard);
                        executionMachine.cancel(shard);
                        counter.getAndIncrement();
                    }
                } else {
                    log.info("shard <{}> is no longer in the registry", shard);
                    executionMachine.cancel(shard);
                    counter.getAndIncrement();
                }
            } catch (Exception e) {
                log.error("shard <" + shard + "> remove from execution machine failure ,cause " + e.getMessage(), e);
                executionMachine.cancel(shard);
                counter.getAndIncrement();
            }
        }
        return counter.get();
    }

    private boolean isVersionOutdated(JobShardDefinition shard) {
        Job job = executionMachine.getJobInstance(shard);
        JobDefinition jobDefinition = definitionRepository.get(shard.getJobId());
        if (job == null) {
            throw new IllegalArgumentException("no such job instance in execution machine");
        }
        long lastUpdateTimeByRegister = jobDefinition.getLastUpdateTime();
        long lastUpdateTimeByExecuting = job.getDefinition().getLastUpdateTime();
        return lastUpdateTimeByExecuting < lastUpdateTimeByRegister;
    }

}
