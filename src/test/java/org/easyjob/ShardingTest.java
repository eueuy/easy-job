package org.easyjob;

import org.easyjob.zookeeper.ZookeeperConfiguration;

import java.io.IOException;

public class ShardingTest {


    public static void main(String[] args) throws IOException {

        ZookeeperConfiguration zkConfig = ZookeeperConfiguration.builder()
                .connectString("10.201.3.102:2181,10.201.3.103:2181,10.201.3.104:2181")
                .build();

        JobNode jobNode1 = new JobNode("easy-job-dev6", "node1", 50, 50, zkConfig);
        System.in.read();

        JobNode jobNode2 = new JobNode("easy-job-dev6", "node2", 50, 50, zkConfig);
        System.in.read();

        JobNode jobNode3 = new JobNode("easy-job-dev6", "node3", 50, 50, zkConfig);
        System.in.read();

        JobNode jobNode4 = new JobNode("easy-job-dev6", "node4", 50, 50, zkConfig);
        System.in.read();

        JobNode jobNode5 = new JobNode("easy-job-dev6", "node5", 50, 50, zkConfig);

        System.in.read();

//
//        for (int i = 0; i < 20; i++) {
//            JobDefinition testJob = JobDefinition.builder()
//                    .jobClassName(TestJob.class.getCanonicalName())
//                    .addParameter("testPara", 123)
//                    .sharding(4)
//                    .build();
//
//            jobNode1.getJobService().submit(testJob);
//        }

        System.in.read();

        jobNode1.shutdown();

        System.in.read();

        jobNode2.shutdown();

        System.in.read();

        jobNode3.shutdown();

        System.in.read();

        jobNode4.shutdown();

        System.in.read();

        jobNode5.shutdown();

    }

}
