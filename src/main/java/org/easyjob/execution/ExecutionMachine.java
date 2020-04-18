package org.easyjob.execution;


import org.easyjob.Job;
import org.easyjob.JobDefinition;
import org.easyjob.allocation.table.JobShardDefinition;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface ExecutionMachine {

    void execute(JobShardDefinition shard, JobDefinition jobDefinition);

    void restart(JobShardDefinition shard, JobDefinition jobDefinition);

    void cancel(JobShardDefinition shard);

    boolean exist(JobShardDefinition shard);

    void cancelAll();

    List<JobShardDefinition> list();

    @Nullable Job getJobInstance(JobShardDefinition shard);

    void shutdown();
}
