/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package org.easyjob;

import lombok.extern.slf4j.Slf4j;
import org.easyjob.allocation.table.JobShardDefinition;
import org.easyjob.execution.ExecutionMachine;

import java.util.Map;

@Slf4j
public abstract class Job {

    private ExecutionMachine executionMachine;
    private JobDefinition definition;
    private JobStatus status;
    private int hasRetriedTime = 0;
    private JobShardDefinition shard;

    public void init(JobDefinition definition, JobShardDefinition shard, ExecutionMachine executionMachine) {
        this.definition = definition;
        this.shard = shard;
        this.executionMachine = executionMachine;
        this.toStart();
        this.toFinished();
    }

    public abstract void onStart(Map<String, Object> parameters, int shardNum);

    public abstract void onStop();

    public abstract void onFinished();

    public void onError(Throwable throwable, boolean doFailover) {
        log.error("job <" + definition.getId() + "> error occurred, cause:" + throwable.getMessage(), throwable);
        if (doFailover) {
            failover();
        }
    }

    private synchronized void failover() {
        if (!(this.hasRetriedTime > definition.getRetryTime())) {
            log.warn("try restart job <{}> ({} time)", definition.getId(), this.hasRetriedTime);
            hasRetriedTime++;
            this.restart();
        } else {
            log.error("job <{}> retry time exhausted", definition.getId());
        }
    }

    public synchronized void restart() {
        log.info("try restart job <{}>", definition.getId());
        //todo 执行机这边应该有已经同步接收成功的概念，只要接收成功，至于任务是否执行失败，任务是否重启，都属于状态，在执行机内部处理。但无论如何不应该再次重复接收已经接收过的任务。
        //todo 因此这行代码是错误的。
        this.executionMachine.restart(this.shard, this.definition);
    }

    public void toFinished() {
        try {
            onFinished();
        } catch (Exception e) {
            toError(e);
        }
        this.status = JobStatus.Finished;
        log.info("job <{}> status to finished", definition.getId());
    }

    public void toStart() {
        log.info("job <{}> status to running", definition.getId());
        this.status = JobStatus.Running;
        try {
            onStart(this.getDefinition().getParameters(), shard.getShardNum());
        } catch (Exception e) {
            toError(e);
        }
    }

    public void toStop() {
        try {
            onStop();
        } catch (Exception e) {
            log.error("job <" + definition.getId() + "> stop failure, cause:" + e.getMessage(), e);
        }
        this.status = JobStatus.Stopped;
        log.info("job <{}> status to stop", definition.getId());
    }

    public void toError(Throwable throwable, boolean doFailover) {
        log.info("job <{}> status to error", definition.getId());
        this.status = JobStatus.Error;
        onError(throwable, doFailover);
    }

    public void toError(Throwable throwable) {
        this.toError(throwable, true);
    }

    public JobStatus getStatus() {
        return this.status;
    }


    public JobDefinition getDefinition() {
        return this.definition;
    }

    public enum JobStatus {
        Running, Error, Stopped, Finished
    }
}