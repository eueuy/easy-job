package org.easyjob.allocation.table;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public
class JobShardDefinition {
    private String jobId;
    private int shardNum;

    @Override
    public String toString() {
        return jobId + "_" + shardNum;
    }


}
