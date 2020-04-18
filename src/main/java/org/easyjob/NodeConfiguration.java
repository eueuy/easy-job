
package org.easyjob;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NodeConfiguration {

    private String nodeId = JobNode.DefaultNodeIdGenerator.getId();
    private String clusterName = "default-cluster";
    private int threadSize = 100;
    private int maxCapacity = 100;

    public static NodeConfigurationBuilder builder() {
        return new NodeConfigurationBuilder();
    }

    public static final class NodeConfigurationBuilder {
        private String nodeId = JobNode.DefaultNodeIdGenerator.getId();
        private String clusterName = "default-cluster";
        private int threadSize = 100;
        private int maxCapacity = 100;

        private NodeConfigurationBuilder() {
        }

        public NodeConfigurationBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public NodeConfigurationBuilder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public NodeConfigurationBuilder threadSize(int threadSize) {
            this.threadSize = threadSize;
            return this;
        }

        public NodeConfigurationBuilder maxCapacity(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        public NodeConfiguration build() {
            NodeConfiguration nodeConfiguration = new NodeConfiguration();
            nodeConfiguration.setNodeId(nodeId);
            nodeConfiguration.setClusterName(clusterName);
            nodeConfiguration.setThreadSize(threadSize);
            nodeConfiguration.setMaxCapacity(maxCapacity);
            return nodeConfiguration;
        }
    }
}
