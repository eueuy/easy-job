package org.easyjob.allocation.rebalance;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.easyjob.membership.Membership;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Rebalance使用固定间隔执行的方式，而不是Event Driven.
 * 因为Rebalance是重量级操作，而节点闪断或者启动集群时节点顺序上线是很正常的，因此不能用Membership Change Event来触发Rebalance，这样会导致Rebalance过于频繁。
 * 该类在固定的间隔时间内触发一次Rebalance检测
 */
@Slf4j
public class RebalanceTrigger {

    private Membership membership;
    private List<Rebalancer> rebalancers = new LinkedList<>();
    private ScheduledFuture<?> scheduledFuture;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("rebalance-trigger-%d").build());

    public RebalanceTrigger(Membership membership) {
        this.membership = membership;
    }

    public void start() {
        scheduledFuture = executor.scheduleWithFixedDelay(this::tryRebalance, 60, 60, TimeUnit.SECONDS);
    }

    public void close() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);

        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    public void addRebalancer(Rebalancer rebalancer) {
        this.rebalancers.add(rebalancer);
    }

    private void tryRebalance() {
        try {
            if (membership.hasLeadership()) {
                for (Rebalancer rebalancer : this.rebalancers) {
                    try {
                        rebalancer.tryRebalance();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
