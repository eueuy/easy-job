package org.easyjob;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.easyjob.util.UUIDUtil;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobDefinition {
    private String id = UUIDUtil.generate8Code();
    private Map<String, Object> parameters;
    private boolean enable = true;
    private int retryTime = 3;
    private int totalShard = 1;
    private String jobClassName;
    private long lastUpdateTime = System.currentTimeMillis();

    public void refreshLastUpdateTime() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id = UUIDUtil.generate8Code();
        private Map<String, Object> parameters = new LinkedHashMap<>();
        private String jobClassName;
        private boolean enable = true;
        private int retryTime = 3;
        private int totalShard = 1;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder enable(boolean enable) {
            this.enable = enable;
            return this;
        }

        public Builder sharding(int totalShard) {
            this.totalShard = totalShard;
            return this;
        }

        public Builder parameters(Map<String, Object> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder addParameter(String key, Object value) {
            this.parameters.put(key, value);
            return this;
        }

        public Builder jobClassName(String className) {
            this.jobClassName = className;
            return this;
        }

        public Builder retryTime(int retryTime) {
            this.retryTime = retryTime;
            return this;
        }

        public JobDefinition build() {
            return new JobDefinition(this.id, this.parameters, this.enable, this.retryTime, this.totalShard,
                    this.jobClassName, 0);
        }
    }
}
