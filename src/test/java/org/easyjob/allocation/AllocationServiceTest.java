package org.easyjob.allocation;

import org.easyjob.allocation.table.AllocationTable;
import org.easyjob.allocation.table.JobShardDefinition;
import org.easyjob.allocation.table.NodeCapacityInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AllocationServiceTest {


    @Test
    public void testConflictAllocation() {

        AllocationService allocationService = new AllocationService(new AllocationTable() {
            @Override
            public void put(String nodeId, JobShardDefinition shard) {

            }

            @Override
            public void removeByJobId(String jobId) {

            }

            @Override
            public void remove(JobShardDefinition shard) {

            }

            @Override
            public boolean containJob(String jobId) {
                return false;
            }

            @Override
            public boolean contain(JobShardDefinition shard) {
                return false;
            }


            @Override
            public @NotNull List<JobShardDefinition> listUnassignedShards() {
                return null;
            }

            @Override
            public void move(JobShardDefinition shard, String sourceNode, String targetNode) {

            }

            @Override
            public String locateNodeByShard(JobShardDefinition shard) {
                return null;
            }

            @Override
            public @NotNull List<JobShardDefinition> listShardsByNode(String nodeId) {
                return null;
            }

            @Override
            public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
                HashMap<String, Integer> result = new HashMap<>();
                result.put("n0", 0);
                result.put("n1", 1);
                result.put("n2", 2);
                result.put("n3", 2);
                result.put("n4", 2);
                return result;
            }

            @Override
            public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
                List<NodeCapacityInfo> nodes = new LinkedList<>();
                nodes.add(new NodeCapacityInfo("n0", 200, 190));
                nodes.add(new NodeCapacityInfo("n1", 200, 110));
                nodes.add(new NodeCapacityInfo("n2", 200, 10));
                nodes.add(new NodeCapacityInfo("n3", 80, 80));
                nodes.add(new NodeCapacityInfo("n4", 80, 10));
                return nodes;
            }

            @Override
            public void applyAvailableOfNodes(List<String> availableNodes) {

            }

            @Override
            public void addNode(String nodeId, int maxCapacity) {

            }
        });

        String target = allocationService.allocateShard(new JobShardDefinition("test", 1));
        Assert.assertEquals(target, "n0");

    }


    @Test
    public void testConflictAllocationWithShardCountMissed() {

        AllocationService allocationService = new AllocationService(new AllocationTable() {
            @Override
            public void put(String nodeId, JobShardDefinition shard) {

            }

            @Override
            public void removeByJobId(String jobId) {

            }

            @Override
            public void remove(JobShardDefinition shard) {

            }

            @Override
            public boolean containJob(String jobId) {
                return false;
            }

            @Override
            public boolean contain(JobShardDefinition shard) {
                return false;
            }

            @Override
            public @NotNull List<JobShardDefinition> listUnassignedShards() {
                return null;
            }

            @Override
            public void move(JobShardDefinition shard, String sourceNode, String targetNode) {

            }

            @Override
            public String locateNodeByShard(JobShardDefinition shard) {
                return null;
            }

            @Override
            public @NotNull List<JobShardDefinition> listShardsByNode(String nodeId) {
                return null;
            }


            @Override
            public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
                HashMap<String, Integer> result = new HashMap<>();
                result.put("n0", 0);
                result.put("n1", 1);
                result.put("n2", 2);
                result.put("n3", 2);
//                result.put("n4", 2);
                return result;
            }

            @Override
            public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
                List<NodeCapacityInfo> nodes = new LinkedList<>();
                nodes.add(new NodeCapacityInfo("n0", 200, 190));
                nodes.add(new NodeCapacityInfo("n1", 200, 110));
                nodes.add(new NodeCapacityInfo("n2", 200, 10));
                nodes.add(new NodeCapacityInfo("n3", 80, 80));
                nodes.add(new NodeCapacityInfo("n4", 80, 10));
                return nodes;
            }

            @Override
            public void applyAvailableOfNodes(List<String> availableNodes) {

            }

            @Override
            public void addNode(String nodeId, int maxCapacity) {

            }
        });

        String target = allocationService.allocateShard(new JobShardDefinition("test", 1));
        Assert.assertEquals(target, "n4");

    }


    @Test
    public void testFirstAllocation() {

        AllocationService allocationService = new AllocationService(new AllocationTable() {
            @Override
            public void put(String nodeId, JobShardDefinition shard) {

            }

            @Override
            public void removeByJobId(String jobId) {

            }

            @Override
            public void remove(JobShardDefinition shard) {

            }

            @Override
            public boolean containJob(String jobId) {
                return false;
            }

            @Override
            public boolean contain(JobShardDefinition shard) {
                return false;
            }


            @Override
            public @NotNull List<JobShardDefinition> listUnassignedShards() {
                return null;
            }

            @Override
            public void move(JobShardDefinition shard, String sourceNode, String targetNode) {

            }

            @Override
            public String locateNodeByShard(JobShardDefinition shard) {
                return null;
            }

            @Override
            public @NotNull List<JobShardDefinition> listShardsByNode(String nodeId) {
                return null;
            }


            @Override
            public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
                HashMap<String, Integer> result = new HashMap<>();
//                result.put("n0", 0);
//                result.put("n1", 0);
//                result.put("n2", 0);
//                result.put("n3", 2);
//                result.put("n4", 2);
                return result;
            }

            @Override
            public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
                List<NodeCapacityInfo> nodes = new LinkedList<>();
                nodes.add(new NodeCapacityInfo("n0", 200, 190));
                nodes.add(new NodeCapacityInfo("n1", 200, 110));
                nodes.add(new NodeCapacityInfo("n2", 200, 10));
                nodes.add(new NodeCapacityInfo("n3", 80, 80));
                nodes.add(new NodeCapacityInfo("n4", 80, 10));
                return nodes;
            }

            @Override
            public void applyAvailableOfNodes(List<String> availableNodes) {

            }

            @Override
            public void addNode(String nodeId, int maxCapacity) {

            }
        });

        String target = allocationService.allocateShard(new JobShardDefinition("test", 1));
        Assert.assertEquals(target, "n2");

    }


    @Test
    public void testNoConflictAllocation() {

        AllocationService allocationService = new AllocationService(new AllocationTable() {
            @Override
            public void put(String nodeId, JobShardDefinition shard) {

            }

            @Override
            public void removeByJobId(String jobId) {

            }

            @Override
            public void remove(JobShardDefinition shard) {

            }

            @Override
            public boolean containJob(String jobId) {
                return false;
            }

            @Override
            public boolean contain(JobShardDefinition shard) {
                return false;
            }


            @Override
            public @NotNull List<JobShardDefinition> listUnassignedShards() {
                return null;
            }

            @Override
            public void move(JobShardDefinition shard, String sourceNode, String targetNode) {

            }

            @Override
            public String locateNodeByShard(JobShardDefinition shard) {
                return null;
            }

            @Override
            public @NotNull List<JobShardDefinition> listShardsByNode(String nodeId) {
                return null;
            }


            @Override
            public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
                HashMap<String, Integer> result = new HashMap<>();
                result.put("n0", 0);
                result.put("n1", 1);
                result.put("n2", 1);
                result.put("n3", 1);
                result.put("n4", 1);
                return result;
            }

            @Override
            public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
                List<NodeCapacityInfo> nodes = new LinkedList<>();
                nodes.add(new NodeCapacityInfo("n0", 200, 190));
                nodes.add(new NodeCapacityInfo("n1", 200, 110));
                nodes.add(new NodeCapacityInfo("n2", 200, 10));
                nodes.add(new NodeCapacityInfo("n3", 80, 80));
                nodes.add(new NodeCapacityInfo("n4", 80, 10));
                return nodes;
            }

            @Override
            public void applyAvailableOfNodes(List<String> availableNodes) {

            }

            @Override
            public void addNode(String nodeId, int maxCapacity) {

            }
        });

        String target = allocationService.allocateShard(new JobShardDefinition("test", 1));
        Assert.assertEquals(target, "n0");

    }


    @Test
    public void testOnlyOneCapacityFreeAllocation() {

        AllocationService allocationService = new AllocationService(new AllocationTable() {
            @Override
            public void put(String nodeId, JobShardDefinition shard) {

            }

            @Override
            public void removeByJobId(String jobId) {

            }

            @Override
            public void remove(JobShardDefinition shard) {

            }

            @Override
            public boolean containJob(String jobId) {
                return false;
            }

            @Override
            public boolean contain(JobShardDefinition shard) {
                return false;
            }


            @Override
            public @NotNull List<JobShardDefinition> listUnassignedShards() {
                return null;
            }

            @Override
            public void move(JobShardDefinition shard, String sourceNode, String targetNode) {

            }

            @Override
            public String locateNodeByShard(JobShardDefinition shard) {
                return null;
            }

            @Override
            public @NotNull List<JobShardDefinition> listShardsByNode(String nodeId) {
                return null;
            }


            @Override
            public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
                HashMap<String, Integer> result = new HashMap<>();
                result.put("n0", 3);
                result.put("n1", 1);
                result.put("n2", 2);
                result.put("n3", 2);
                result.put("n4", 2);
                return result;
            }

            @Override
            public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
                List<NodeCapacityInfo> nodes = new LinkedList<>();
                nodes.add(new NodeCapacityInfo("n0", 200, 190));
                nodes.add(new NodeCapacityInfo("n1", 200, 200));
                nodes.add(new NodeCapacityInfo("n2", 200, 200));
                nodes.add(new NodeCapacityInfo("n3", 80, 80));
                nodes.add(new NodeCapacityInfo("n4", 80, 80));
                return nodes;
            }

            @Override
            public void applyAvailableOfNodes(List<String> availableNodes) {

            }

            @Override
            public void addNode(String nodeId, int maxCapacity) {

            }
        });

        String target = allocationService.allocateShard(new JobShardDefinition("test", 1));
        Assert.assertEquals(target, "n0");

    }


    @Test
    public void testNoCapacityAllocation() {

        AllocationService allocationService = new AllocationService(new AllocationTable() {
            @Override
            public void put(String nodeId, JobShardDefinition shard) {

            }

            @Override
            public void removeByJobId(String jobId) {

            }

            @Override
            public void remove(JobShardDefinition shard) {

            }

            @Override
            public boolean containJob(String jobId) {
                return false;
            }

            @Override
            public boolean contain(JobShardDefinition shard) {
                return false;
            }


            @Override
            public @NotNull List<JobShardDefinition> listUnassignedShards() {
                return null;
            }

            @Override
            public void move(JobShardDefinition shard, String sourceNode, String targetNode) {

            }

            @Override
            public String locateNodeByShard(JobShardDefinition shard) {
                return null;
            }

            @Override
            public @NotNull List<JobShardDefinition> listShardsByNode(String nodeId) {
                return null;
            }


            @Override
            public Map<String, Integer> fetchJobsAllocationTable(String jobId) {
                HashMap<String, Integer> result = new HashMap<>();
                result.put("n0", 0);
                result.put("n1", 1);
                result.put("n2", 2);
                result.put("n3", 2);
                result.put("n4", 2);
                return result;
            }

            @Override
            public @NotNull List<NodeCapacityInfo> fetchNodeCapacityInfos() {
                List<NodeCapacityInfo> nodes = new LinkedList<>();
                nodes.add(new NodeCapacityInfo("n0", 200, 200));
                nodes.add(new NodeCapacityInfo("n1", 200, 200));
                nodes.add(new NodeCapacityInfo("n2", 200, 200));
                nodes.add(new NodeCapacityInfo("n3", 80, 80));
                nodes.add(new NodeCapacityInfo("n4", 80, 80));
                return nodes;
            }

            @Override
            public void applyAvailableOfNodes(List<String> availableNodes) {

            }

            @Override
            public void addNode(String nodeId, int maxCapacity) {

            }
        });

        try {
            String target = allocationService.allocateShard(new JobShardDefinition("test", 1));
        } catch (Exception e) {
            Assert.assertEquals(e.getClass().getCanonicalName(), IllegalStateException.class.getCanonicalName());
        }

    }

}