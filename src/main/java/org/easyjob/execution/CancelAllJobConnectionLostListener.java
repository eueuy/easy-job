package org.easyjob.execution;

import lombok.extern.slf4j.Slf4j;
import org.easyjob.zookeeper.ConnectionLostListener;

@Slf4j
public class CancelAllJobConnectionLostListener implements ConnectionLostListener {

    private ExecutionMachine executionMachine;

    public CancelAllJobConnectionLostListener(ExecutionMachine executionMachine) {
        this.executionMachine = executionMachine;
    }

    @Override
    public void connectionHasLost() {
        log.warn("im leaved, i will cancel all my jobs");
        executionMachine.cancelAll();
    }
}
