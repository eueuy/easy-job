package org.easyjob;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class TestFailoverJob extends Job {

    @Override
    public void onStart(Map<String, Object> parameters, int shardNum) {
        log.info("test job <{}> started at shard {}", this.getDefinition().getId(), shardNum);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException ignore) {
        }

        toError(new IllegalStateException("some thing wrong"));
        Thread.currentThread().stop();
    }

    @Override
    public void onStop() {
        log.info("test job <{}> stopped", this.getDefinition().getId());
    }

    @Override
    public void onFinished() {
        log.info("test job <{}> finished:", this.getDefinition().getId());
    }

}
