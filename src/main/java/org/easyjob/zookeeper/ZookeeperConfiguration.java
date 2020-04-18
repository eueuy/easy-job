package org.easyjob.zookeeper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ZookeeperConfiguration {
    private String connectString = "localhost:2181";
    private int sessionTimeoutMs = 6000;
    private int connectionTimeoutMs = 3000;
    private int retrySleepTimeMs = 3000;
    private int retryMaxRetries = 3;


    public static ZookeeperConfigurationBuilder builder() {
        return new ZookeeperConfigurationBuilder();
    }

    public static final class ZookeeperConfigurationBuilder {
        private String connectString;
        private int sessionTimeoutMs = 6000;
        private int connectionTimeoutMs = 3000;
        private int retrySleepTimeMs = 3000;
        private int retryMaxRetries = 3;

        private ZookeeperConfigurationBuilder() {
        }

        public ZookeeperConfigurationBuilder connectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        public ZookeeperConfigurationBuilder sessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        public ZookeeperConfigurationBuilder connectionTimeoutMs(int connectionTimeoutMs) {
            this.connectionTimeoutMs = connectionTimeoutMs;
            return this;
        }

        public ZookeeperConfigurationBuilder retrySleepTimeMs(int retrySleepTimeMs) {
            this.retrySleepTimeMs = retrySleepTimeMs;
            return this;
        }

        public ZookeeperConfigurationBuilder retryMaxRetries(int retryMaxRetries) {
            this.retryMaxRetries = retryMaxRetries;
            return this;
        }

        public ZookeeperConfiguration build() {
            ZookeeperConfiguration zkConfiguration = new ZookeeperConfiguration();
            zkConfiguration.setConnectString(connectString);
            zkConfiguration.setSessionTimeoutMs(sessionTimeoutMs);
            zkConfiguration.setConnectionTimeoutMs(connectionTimeoutMs);
            zkConfiguration.setRetrySleepTimeMs(retrySleepTimeMs);
            zkConfiguration.setRetryMaxRetries(retryMaxRetries);
            return zkConfiguration;
        }
    }
}
