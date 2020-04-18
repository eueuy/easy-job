package org.easyjob.allocation;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.membership.LeaderChangedListener;

import java.util.List;

@Slf4j
public class ApplyAvailableLeaderChangedListener implements LeaderChangedListener {

    private AllocationTable allocationTable;

    public ApplyAvailableLeaderChangedListener(AllocationTable allocationTable) {
        this.allocationTable = allocationTable;
    }

    @Override
    public void gotLeadership(List<String> newNodeList) {
        log.warn("i just take leader, i will apply new node list first,the new node list is :{}", JSON.toJSONString(newNodeList));
        allocationTable.applyAvailableOfNodes(newNodeList);
    }
}
