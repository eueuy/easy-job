package org.easyjob;

import org.easyjob.zookeeper.ZookeeperConfiguration;

import java.io.IOException;
import java.util.UUID;

public class QuickStartDemo {


    public static void main(String[] args) throws IOException {

        ZookeeperConfiguration zkConfig = ZookeeperConfiguration.builder()
                .connectString("10.201.3.102:2181,10.201.3.103:2181,10.201.3.104:2181")
                .build();

        JobNode jobNode1 = new JobNode("easy-job-dev6", UUID.randomUUID().toString(), 50, 2, zkConfig);

        System.in.read();

        jobNode1.shutdown();

    }

}
