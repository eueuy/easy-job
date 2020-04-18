package org.easyjob;

import org.easyjob.zookeeper.ZookeeperConfiguration;

import java.io.IOException;

public class QuickStartDemo {


    public static void main(String[] args) throws IOException {

        ZookeeperConfiguration zkConfig = ZookeeperConfiguration.builder()
                .connectString("localhost:9092")
                .build();

        JobNode jobNode1 = new JobNode("easy-job-1", 50, 2, zkConfig);
        JobNode jobNode2 = new JobNode("easy-job-2", 50, 2, zkConfig);

        JobDefinition someJob = JobDefinition.builder()
                .jobClassName(TestJob.class.getCanonicalName())
                .addParameter("someParameter", 123)
                .sharding(4)
                .build();

        jobNode1.getJobService().submit(someJob);

        System.in.read();

        jobNode1.shutdown();
        jobNode2.shutdown();

    }

}
