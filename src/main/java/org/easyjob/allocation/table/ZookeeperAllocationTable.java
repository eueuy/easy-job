package org.easyjob.allocation.table;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ZookeeperAllocationTable implements AllocationTable {
    private static final String UNAVAILABLE_MARK = "unassigned";
    private CuratorFramework client;

    public ZookeeperAllocationTable(CuratorFramework client) {
        this.client = client;
        tryCreatePartitionPath();
        log.info("Allocation Table Inited");
    }

    private void tryCreatePartitionPath() {
        try {
            if (client.checkExists().forPath("/partition") == null) {
                client.create().creatingParentsIfNeeded().forPath("/partition");
            }
        } catch (Exception e) {
            throw new AllocationTableException("create partition path failure cause:" + e.getMessage(), e);
        }
    }

    @Override
    public void put(String nodeId, JobShardDefinition shard) {
        if (!containedNode(nodeId)) {
            log.error("node not found :" + nodeId);
            throw new IllegalArgumentException("node not found :" + nodeId);
        }

        try {
            client.create().creatingParentsIfNeeded()
                    .forPath("/partition/" + nodeId + "/" + shard.getJobId() + "/" + shard.getShardNum());
            log.info("shard <{}> has put to node <{}>", shard, nodeId);
        } catch (Exception e) {
            log.error("shard <" + shard + "> put error ,cause " + e.getMessage(), e);
            throw new AllocationTableException("put shard " + shard + " failure cause : " + e.getMessage(), e);
        }
    }

    @Override
    public void removeByJobId(String jobId) {
        listNodes().forEach(node -> removeJob(node, jobId));
        log.info("job <{}> has been removed", jobId);
    }

    @Override
    public void remove(JobShardDefinition shard) {
        listNodes().forEach(node -> remove(node, shard));
    }

    @Override
    public boolean containJob(String jobId) {
        for (String node : this.listNodes()) {
            try {
                Stat stat = client.checkExists().forPath("/partition/" + node + "/" + jobId);
                if (stat != null && stat.getNumChildren() > 0) {
                    return true;
                }
            } catch (Exception e) {
                log.error("deciding job " + jobId + "contain error , cause:" + e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public boolean contain(JobShardDefinition shard) {
        for (String node : listNodes()) {
            if (contain(node, shard)) {
                return true;
            }
        }
        return false;
    }

    public void remove(String nodeId, JobShardDefinition shard) {
        try {
            client.delete().forPath("/partition/" + nodeId + "/" + shard.getJobId() + "/" + shard.getShardNum());
        } catch (Exception e) {
            throw new AllocationTableException(e);
        }
        log.info("shard <{}> has been removed", shard);
    }

    private void removeJob(String nodeId, String jobId) {
        try {
            Stat stat = client.checkExists().forPath("/partition/" + nodeId + "/" + jobId);
            if (stat != null) {
                client.delete().deletingChildrenIfNeeded().forPath("/partition/" + nodeId + "/" + jobId);
                log.info("job <{}> has been removed from node <{}>", jobId, nodeId);
            }
        } catch (Exception e) {
            log.error("remove job failure :" + jobId + " cause:" + e.getMessage(), e);
            throw new AllocationTableException("remove job failure :" + jobId + " cause:" + e.getMessage(), e);
        }
    }

    private boolean contain(String nodeId, JobShardDefinition shard) {
        try {
            Stat stat = client.checkExists().forPath("/partition/" + nodeId + "/" + shard.getJobId() + "/" + shard.getShardNum());
            if (stat != null) {
                return true;
            }
        } catch (Exception e) {
            log.error("can not decided shard " + shard + "exists cause:" + e.getMessage(), e);
            throw new AllocationTableException("can not decided exists cause:" + e.getMessage(), e);
        }
        return false;
    }

    @NotNull
    @Override
    public List<JobShardDefinition> listShardsByNode(String nodeId) {
        if (containedNode(nodeId)) {
            try {
                List<JobShardDefinition> result = new LinkedList<>();
                List<String> jobs = client.getChildren().forPath("/partition/" + nodeId);
                jobs.forEach(jobId -> {
                    try {
                        List<String> shardNums = client.getChildren().forPath("/partition/" + nodeId + "/" + jobId);
                        shardNums.forEach(shardNum -> {
                            JobShardDefinition jobShardDefinition = new JobShardDefinition(jobId, Integer.valueOf(shardNum));
                            result.add(jobShardDefinition);
                        });
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
                return result;
            } catch (Exception e) {
                throw new AllocationTableException(e);
            }
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
        Map<String, Integer> result = new HashMap<>();
        for (String node : listNodes()) {
            try {
                Stat stat = client.checkExists().forPath("/partition/" + node + "/" + jobId);
                if (stat != null) {
                    result.put(node, stat.getNumChildren());
                } else {
                    result.put(node, 0);
                }
            } catch (Exception e) {
                log.error("count shard allocated of nodes error , cause : " + e.getMessage(), e);
            }
        }
        return result;
    }

    @NotNull
    @Override
    public List<JobShardDefinition> listUnassignedShards() {
        return listUnavailableNodes().stream().flatMap((Function<String, Stream<JobShardDefinition>>) s -> {
            try {
                return listShardsByNode(s).stream();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            return Stream.empty();
        }).collect(Collectors.toList());
    }

    @Override
    //todo need consider transactions
    public void move(JobShardDefinition shard, String sourceNode, String targetNode) {
        if (sourceNode.equals(targetNode)) {
            throw new IllegalArgumentException("the source and target node is same");
        }
        if (this.contain(targetNode, shard)) {
            throw new IllegalStateException("shard " + shard + " already exist on target node " + targetNode);
        }
        this.put(targetNode, shard);
        this.remove(sourceNode, shard);
        log.info("shard " + shard + " has been moved from node<" + sourceNode + "> to node <" + targetNode + ">");
    }

    @Override
    @Nullable
    public String locateNodeByShard(JobShardDefinition shard) {
        for (String node : listNodes()) {
            if (contain(node, shard)) {
                return node;
            }
        }
        return null;
    }

    /* nodes operations bellow*/

    @Override
    public synchronized void addNode(String nodeId, int maxCapacity) {
        if (containedNode(nodeId)) {
            markNodeAsAvailable(nodeId, maxCapacity);
        } else {
            try {
                client.create().creatingParentsIfNeeded().forPath("/partition/" + nodeId,
                        String.valueOf(maxCapacity).getBytes());
            } catch (Exception e) {
                throw new AllocationTableException("add node failure " + nodeId, e);
            }
            log.info("node <{}> added to registry", nodeId);
        }
    }

    private boolean containedNode(String nodeId) {
        try {
            Stat stat = client.checkExists().forPath("/partition/" + nodeId);
            return stat != null;
        } catch (Exception e) {
            throw new AllocationTableException(e);
        }
    }

    private List<String> listNodes() {
        try {
            return client.getChildren().forPath("/partition");
        } catch (Exception e) {
            throw new AllocationTableException(e);
        }
    }

    /**
     * try to remove a empty node , if have any jobs assigned on it, cancel the remove action
     */
    private synchronized void removeNode(String nodeId) {
        if (containedNode(nodeId)) {
            try {
                client.delete().deletingChildrenIfNeeded().forPath("/partition/" + nodeId);
                log.info("node " + nodeId + " removed");
            } catch (Exception e) {
                throw new AllocationTableException(e);
            }
        }
    }

    /**
     * 按给定的 node list 来调整node 的 available
     */
    @Override
    public synchronized void applyAvailableOfNodes(List<String> availableNodes) {
        log.info("try apply new node list:{}", JSON.toJSONString(availableNodes));

        this.listNodes().forEach(nodeFromRegistry -> {
            try {
                if (!availableNodes.contains(nodeFromRegistry)) {
                    this.markNodeAsUnavailable(nodeFromRegistry);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        cleanUnavailableNode();
        log.info("new node list applied");
    }

    private void cleanUnavailableNode() {
        this.listUnavailableNodes().forEach(node -> {
            if (listShardsByNode(node).size() < 1) {
                removeNode(node);
            }
        });
    }

    private synchronized void markNodeAsUnavailable(String nodeId) {
        if (containedNode(nodeId) && !isUnavailableNode(nodeId)) {
            try {
                client.setData().forPath("/partition/" + nodeId, UNAVAILABLE_MARK.getBytes());
                log.info("node " + nodeId + " marked as unavailable");
            } catch (Exception e) {
                throw new AllocationTableException(e);
            }
        }
    }

    private synchronized void markNodeAsAvailable(String nodeId, int maxCapacity) {
        if (containedNode(nodeId)) {
            try {
                client.setData().forPath("/partition/" + nodeId, String.valueOf(maxCapacity).getBytes());
                log.info("node " + nodeId + " marked as available");
            } catch (Exception e) {
                throw new AllocationTableException(e);
            }
        }
    }


    @Override
    public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
        List<NodeCapacityInfo> result = new ArrayList<>();
        listAvailableNodes().forEach(nodeName -> {
            try {
                Stat stat = client.checkExists().forPath("/partition/" + nodeName);
                if (stat != null) {
                    int maxCapacity = Integer.valueOf(new String(client.getData().forPath("/partition/" + nodeName)));
                    int jobsNum = stat.getNumChildren();
                    result.add(new NodeCapacityInfo(nodeName, maxCapacity, jobsNum));
                } else {
                    log.error("node may not exist when getJobInstance node's capacity");
                }
            } catch (Exception e) {
                log.error("fetch node<" + nodeName + ">'s capacity info failure, cause: " + e.getMessage(), e);
            }
        });
        return result;
    }

    @NotNull
    private List<String> listAvailableNodes() {
        return listNodes().stream().filter(node -> {
                    try {
                        return !isUnavailableNode(node);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        return false;
                    }
                }
        ).collect(Collectors.toList());
    }


    private List<String> listUnavailableNodes() {
        return listNodes().stream().filter(node -> {
                    try {
                        return isUnavailableNode(node);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        return false;
                    }
                }
        ).collect(Collectors.toList());
    }

    private boolean isUnavailableNode(String nodeId) {
        try {
            String mark = new String(client.getData().forPath("/partition/" + nodeId));
            return mark.equals(UNAVAILABLE_MARK);
        } catch (Exception e) {
            throw new AllocationTableException(e);
        }
    }
}


















