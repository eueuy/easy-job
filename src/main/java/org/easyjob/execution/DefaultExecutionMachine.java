package org.easyjob.execution;

import lombok.extern.slf4j.Slf4j;
import org.easyjob.Job;
import org.easyjob.JobDefinition;
import org.easyjob.allocation.table.JobShardDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class DefaultExecutionMachine implements ExecutionMachine {

    private Map<JobShardDefinition, Job> jobInstanceRegistry = new ConcurrentHashMap<>();
    private ExecutorService executor;

    public DefaultExecutionMachine(int threadSize) {
        this.executor = Executors.newFixedThreadPool(threadSize);
        log.info("default execution machine inited");
    }

    @Override
    public void execute(JobShardDefinition shard, JobDefinition jobDefinition) {
        if (exist(shard)) {
            throw new IllegalArgumentException("job already submitted");
        }

        Job jobInstance = createJobInstance(jobDefinition);
        executor.execute(() -> jobInstance.init(jobDefinition, shard, this));
        jobInstanceRegistry.put(shard, jobInstance);
        log.info("job shard <{}> started", shard);
    }

    @Override
    public void restart(JobShardDefinition shard, JobDefinition jobDefinition) {
        this.cancel(shard);
        this.execute(shard, jobDefinition);
    }

    private Job createJobInstance(JobDefinition jobDefinition) {
        try {
            Class<?> handlerClass = Class.forName(jobDefinition.getJobClassName());
            return (Job) handlerClass.newInstance();
        } catch (final ReflectiveOperationException ex) {
            log.error("can not create job <{}>'s instance cause class name is illegal", jobDefinition.getId());
            throw new IllegalArgumentException("class name is illegal:" + jobDefinition.getJobClassName());
        }
    }

    @Override
    public void cancel(JobShardDefinition shard) {
        if (jobInstanceRegistry.containsKey(shard)) {
            //job instance's operation must be asynchronous
            Job job = jobInstanceRegistry.get(shard);
            if (job != null) {
                executor.execute(job::toStop);
            }
            jobInstanceRegistry.remove(shard);
        }
        log.info("job <{}> canceled", shard);
    }

    @Override
    public boolean exist(JobShardDefinition shard) {
        return jobInstanceRegistry.containsKey(shard);
    }

    @Override
    public void cancelAll() {
        jobInstanceRegistry.keySet().forEach(this::cancel);
    }

    @Override
    public List<JobShardDefinition> list() {
        return new ArrayList<>(jobInstanceRegistry.keySet());
    }

    @Override
    public Job getJobInstance(JobShardDefinition shard) {
        return jobInstanceRegistry.get(shard);
    }

    @Override
    public void shutdown() {
        cancelAll();
        if (executor != null) {
            executor.shutdown();
        }
    }
}
