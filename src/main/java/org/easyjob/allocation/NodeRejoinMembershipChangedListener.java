package org.easyjob.allocation;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.easyjob.NodeConfiguration;
import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.execution.ExecutionMachine;
import org.easyjob.membership.MembershipChangedListener;

import java.util.List;

@Slf4j
public class NodeRejoinMembershipChangedListener implements MembershipChangedListener {

    //todo membership changed listener 能否完全值得信赖，是否需要通过定时器来保证
    // 定时器是指， 无论是否发生了 membership changed , 隔一段时间都尝试进行一次如下操作
    //  register 和 apply 这块可以定时执行反正是幂等的。 但是leave里的 cancelAll 必须依靠 listener

    private AllocationTable allocationTable;
    private ExecutionMachine executionMachine;
    private NodeConfiguration nodeConfiguration;

    public NodeRejoinMembershipChangedListener(NodeConfiguration nodeConfiguration, AllocationTable allocationTable, ExecutionMachine executionMachine) {
        this.allocationTable = allocationTable;
        this.executionMachine = executionMachine;
        this.nodeConfiguration = nodeConfiguration;
    }

    @Override
    public void join(boolean hasLeadership, boolean isMine, String changedNodeId, List<String> newNodeList) {
        if (isMine) {
            log.warn("im <{}> join , i will add myself to registry first. new node list is : {}", changedNodeId, JSON.toJSONString(newNodeList));
            allocationTable.addNode(nodeConfiguration.getNodeId(), nodeConfiguration.getMaxCapacity());
        }

    }

    @Override
    public void leave(boolean hasLeadership, boolean isMine, String changedNodeId, List<String> newNodeList) {
        if (isMine) {
            log.warn("im <{}> leaved, i will cancel all my jobs. new node list is {}", changedNodeId, JSON.toJSONString(newNodeList));
            executionMachine.cancelAll();
        }

    }
}
