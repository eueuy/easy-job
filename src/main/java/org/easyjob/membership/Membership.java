package org.easyjob.membership;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@Slf4j
public class Membership {

    private final List<MembershipChangedListener> membershipChangedListeners = new LinkedList<>();
    private final List<LeaderChangedListener> leaderChangedListeners = new LinkedList<>();

    private CuratorFramework client;
    private final CountDownLatch leaderLatch = new CountDownLatch(1);
    private LeaderSelector leaderSelector;
    private PathChildrenCache nodesCache;


    public Membership(CuratorFramework client) {
        this.client = client;
        initMembershipWatcher();
        log.info("membership inited");
    }

    private void waitForAsyncNodeCreation() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ignore) {
        }
    }

    public void addMembershipChangedListener(MembershipChangedListener membershipChangedListener) {
        membershipChangedListeners.add(membershipChangedListener);
    }

    public void addLeaderChangedListener(LeaderChangedListener leaderChangedListener) {
        leaderChangedListeners.add(leaderChangedListener);
    }

    public void join(String nodeId) {
        //leader selector init
        LeaderSelector selector = new LeaderSelector(client, "/nodes", new LeaderSelectorListenerAdapter() {
            public void takeLeadership(CuratorFramework client) {
                log.warn(nodeId + " is the leader now");
                try {
                    leaderChangedListeners.forEach(listener -> {
                        try {
                            listener.gotLeadership(getNodeList());
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    });

                    leaderLatch.await();
                } catch (InterruptedException ignore) {
                }
                log.warn(nodeId + " has quit the leader");
            }
        });

        selector.setId(nodeId);
        selector.autoRequeue();
        selector.start();
        this.leaderSelector = selector;
        waitForAsyncNodeCreation();
        log.info("membership leader elector inited");
    }

    private void initMembershipWatcher() {
        //nodes watcher init
        PathChildrenCache cache = new PathChildrenCache(client, "/nodes", true);
        cache.getListenable().addListener(
                (client1, event) -> {

                    String changedNodeId = fetchNodeId(event);
                    boolean isMine = getNodeId().equals(changedNodeId);

                    log.info("membership changed, cause:" + event.getType().toString() +
                            ". by node" + changedNodeId +
                            ". now membership is " + JSON.toJSONString(getNodeList())
                    );

                    for (MembershipChangedListener listener : membershipChangedListeners) {
                        try {
                            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                                listener.join(hasLeadership(), isMine, changedNodeId, getNodeList());
                            }
                            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                                listener.leave(hasLeadership(), isMine, changedNodeId, getNodeList());
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
        );
        try {
            cache.start();
        } catch (Exception e) {
            throw new MemberShipException(e.getMessage(), e);
        }
        this.nodesCache = cache;
        log.info("membership watcher inited");
    }

    private String fetchNodeId(PathChildrenCacheEvent event) {
        if (event.getData() != null) {
            return new String(event.getData().getData());
        } else {
            return "unknown";
        }
    }


    private List<String> getNodeList() {
        try {
            return leaderSelector.getParticipants().stream().map(Participant::getId).collect(Collectors.toList());
        } catch (Exception e) {
            throw new MemberShipException(e.getMessage(), e);
        }
    }

    public boolean hasLeadership() {
        return leaderSelector.hasLeadership();
    }

    public String getLeaderId() {
        try {
            return leaderSelector.getLeader().getId();
        } catch (Exception e) {
            throw new MemberShipException(e.getMessage(), e);
        }
    }

    public String getNodeId() {
        return leaderSelector.getId();
    }

    public void close() {
        try {
            nodesCache.close();
            leaderSelector.close();
        } catch (IOException e) {
            throw new MemberShipException(e.getMessage(), e);
        }
    }

}
