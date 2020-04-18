package org.easyjob;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class TestJob extends Job {

    private final CountDownLatch runningLatch = new CountDownLatch(1);

    @Override
    public void onStart(Map<String, Object> parameters, int shardNum) {
        log.info("test job <{}> started at shard {}", this.getDefinition().getId(), shardNum);
        try {
            runningLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStop() {
        log.info("test job <{}> stopped", this.getDefinition().getId());
        runningLatch.countDown();
    }

    @Override
    public void onFinished() {
        log.info("test job <{}> finished:", this.getDefinition().getId());
    }

}
