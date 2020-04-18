package org.easyjob;


import com.alibaba.fastjson.JSON;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

@SuppressWarnings("Duplicates")
class ElectorTest {

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


        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println("im the king!");
                // this callback will getJobInstance called when you are the leader
                // do whatever leader work you need to and only exit
                // this method when you want to relinquish leadership
                System.in.read();
            }

        };

        LeaderSelector selector = new LeaderSelector(client, "/elect", listener);
        selector.autoRequeue();  // not required, but this is behavior that you will probably expect
        selector.start();

        PathChildrenCache cache = new PathChildrenCache(client, "/elect", true);
        cache.getListenable().addListener(
                (client1, event) -> {
                    String ip = new String(event.getData().getData());
                    System.out.println(event.getType() + ": " + ip);
                    List<String> strings = client.getChildren().forPath("/elect");
                    System.out.println(JSON.toJSON(strings));
                }
        );
        cache.start();


    }


}