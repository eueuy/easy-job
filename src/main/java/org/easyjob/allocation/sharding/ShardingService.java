package org.easyjob.allocation.sharding;

import org.easyjob.JobDefinition;
import org.easyjob.allocation.table.JobShardDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;

public class ShardingService {

    @NotNull
    public static List<JobShardDefinition> generateShards(JobDefinition jobDefinition) {
        LinkedList<JobShardDefinition> result = new LinkedList<>();
        for (int shardNum = 0; shardNum < jobDefinition.getTotalShard(); shardNum++) {
            JobShardDefinition jobShardInfo = new JobShardDefinition(jobDefinition.getId(), shardNum);
            result.add(jobShardInfo);
        }
        return result;
    }

}
