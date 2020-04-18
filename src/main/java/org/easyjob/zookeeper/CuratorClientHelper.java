package org.easyjob.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CuratorClientHelper {

    private CuratorFramework client;

    private List<ConnectionLostListener> connectionLostListeners = new LinkedList<>();

    public void addConnectionLostListener(ConnectionLostListener connectionLostListener) {
        this.connectionLostListeners.add(connectionLostListener);
    }

    public CuratorClientHelper(String clusterName, ZookeeperConfiguration zookeeperConfiguration) {
        Objects.requireNonNull(clusterName);
        Objects.requireNonNull(zookeeperConfiguration);

        this.client = buildCuratorFramework(clusterName, zookeeperConfiguration);

        client.getConnectionStateListenable().addListener((curatorFramework, newState) -> {
            if (newState == ConnectionState.LOST) {
                try {
                    connectionLostListeners.forEach(ConnectionLostListener::connectionHasLost);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        });

    }

    private CuratorFramework buildCuratorFramework(String clusterName, ZookeeperConfiguration zookeeperConfiguration) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(zookeeperConfiguration.getRetrySleepTimeMs(), zookeeperConfiguration.getRetryMaxRetries());
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zookeeperConfiguration.getConnectString())
                .sessionTimeoutMs(zookeeperConfiguration.getSessionTimeoutMs())
                .connectionTimeoutMs(zookeeperConfiguration.getConnectionTimeoutMs())
                .retryPolicy(retryPolicy)
                .namespace("easy-job-" + clusterName)
                .build();

        client.start();

        try {
            client.blockUntilConnected(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        log.warn("curator framework client build success");
        return client;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void close() {
        client.close();
    }
}
