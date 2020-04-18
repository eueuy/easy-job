package org.easyjob;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;

@SuppressWarnings("Duplicates")
class CuratorFrameworkTest {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString("localhost:9092")
                        .sessionTimeoutMs(5000)
                        .connectionTimeoutMs(5000)
                        .retryPolicy(retryPolicy)
                        .namespace("easy-job-test")
                        .build();

        client.start();


        Stat stat = client.checkExists().forPath("/allocate");
        if (stat == null) {
            client.create().forPath("/allocate", "init".getBytes("UTF-8"));
        } else {
            System.out.println("=================" + stat.getNumChildren());
        }

        String ip = InetAddress.getLocalHost().getHostAddress();

        PathChildrenCache cache = new PathChildrenCache(client, "/allocate/" + ip, true);
        cache.getListenable().addListener(
                (client1, event) -> {
                    System.out.println(event.getType());
                    System.out.println(event.getData().getPath());
                    System.out.println(new String(event.getData().getData()));
                }
        );
        cache.start();


        System.in.read();

    }


}