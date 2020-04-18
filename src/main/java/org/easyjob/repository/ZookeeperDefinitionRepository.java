package org.easyjob.repository;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.data.Stat;
import org.easyjob.JobDefinition;
import org.easyjob.membership.MemberShipException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Slf4j
public class ZookeeperDefinitionRepository implements DefinitionRepository {

    private CuratorFramework client;
    private List<DefinitionChangedListener> listeners = new CopyOnWriteArrayList<>();
    private PathChildrenCache nodesCache;

    public ZookeeperDefinitionRepository(CuratorFramework client) {
        this.client = client;
        tryCreateRepositoryPath();
        initListeners();
    }

    private void tryCreateRepositoryPath() {
        try {
            if (client.checkExists().forPath("/jobs") == null) {
                client.create().forPath("/jobs");
            }
        } catch (Exception e) {
            throw new DefinitionRepositoryException("create partition path failure cause:" + e.getMessage(), e);
        }
    }

    @Override
    public void save(JobDefinition jobDefinition) {
        if (contain(jobDefinition.getId())) {
            throw new IllegalArgumentException("job already exist");
        }

        try {
            client.create().creatingParentsIfNeeded().forPath("/jobs/" + jobDefinition.getId(), JSON.toJSONString(jobDefinition).getBytes());
            log.info("job <{}> has saved", jobDefinition.getId());
        } catch (Exception e) {
            log.error("job <" + jobDefinition.getId() + "> put error ,cause " + e.getMessage(), e);
            throw new DefinitionRepositoryException("save job " + jobDefinition.getId() + " failure cause : " + e.getMessage(), e);
        }
    }

    @Override
    public void update(String jobId, JobDefinition jobDefinition) {
        if (this.contain(jobId)) {
            jobDefinition.setId(jobId);
            jobDefinition.refreshLastUpdateTime();
            try {
                client.setData().forPath("/jobs/" + jobDefinition.getId(), JSON.toJSONString(jobDefinition).getBytes());
                log.info("job <{}> has been updated", jobDefinition.getId());
            } catch (Exception e) {
                log.error("job <" + jobId + "> update failure cause" + e.getMessage(), e);
                throw new DefinitionRepositoryException("update failure cause : " + e.getMessage(), e);
            }
        } else {
            log.error("job " + jobId + " not exist");
            throw new IllegalArgumentException("job " + jobId + " not exist");
        }
    }

    @Override
    public void remove(String jobId) {
        if (this.contain(jobId)) {
            try {
                client.delete().forPath("/jobs/" + jobId);
                log.info("job <{}> has been removed", jobId);
            } catch (Exception e) {
                log.error("remove job failure :" + jobId + " cause:" + e.getMessage(), e);
                throw new DefinitionRepositoryException("remove job failure :" + jobId + " cause:" + e.getMessage(), e);
            }
        }
    }

    @Override
    public JobDefinition get(String jobId) {
        if (this.contain(jobId)) {
            try {
                String content = new String(client.getData().forPath("/jobs/" + jobId));
                return JSONObject.parseObject(content, JobDefinition.class);
            } catch (Exception e) {
                log.error("get job failure :" + jobId + " cause:" + e.getMessage(), e);
                throw new DefinitionRepositoryException("get job failure :" + jobId + " cause:" + e.getMessage(), e);
            }
        } else {
            throw new IllegalArgumentException("no such job");
        }
    }

    @Override
    public boolean contain(String jobId) {
        try {
            Stat stat = client.checkExists().forPath("/jobs/" + jobId);
            if (stat != null) {
                return true;
            }
        } catch (Exception e) {
            log.error("can not decided job " + jobId + "exists cause:" + e.getMessage(), e);
            throw new DefinitionRepositoryException("can not decided exists cause:" + e.getMessage(), e);
        }
        return false;
    }

    @Override
    public List<JobDefinition> list() {
        return listId().stream().map(this::get).collect(Collectors.toList());
    }

    @Override
    public List<String> listId() {
        try {
            return client.getChildren().forPath("/jobs");
        } catch (Exception e) {
            throw new DefinitionRepositoryException(e);
        }
    }

    @Override
    public void addListener(DefinitionChangedListener listener) {
        this.listeners.add(listener);
    }

    private void initListeners() {
        PathChildrenCache cache = new PathChildrenCache(client, "/jobs", true);
        cache.getListenable().addListener(
                (client1, event) -> {
                    for (DefinitionChangedListener listener : listeners) {
                        try {
                            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                                listener.created(getJobDefinitionByEvent(event));
                            }
                            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                                listener.removed(getJobDefinitionByEvent(event));
                            }
                            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                                listener.updated(getJobDefinitionByEvent(event));
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
        );
        try {
            cache.start();
        } catch (Exception e) {
            throw new MemberShipException(e.getMessage(), e);
        }
        this.nodesCache = cache;
    }

    private JobDefinition getJobDefinitionByEvent(PathChildrenCacheEvent event) {
        if (event.getData() != null) {
            return JSONObject.parseObject(new String(event.getData().getData()), JobDefinition.class);
        } else {
            throw new IllegalStateException("can not deserialization the job definition in event");
        }
    }

    @Override
    public void close() {
        try {
            this.nodesCache.close();
        } catch (IOException e) {
            throw new DefinitionRepositoryException(e.getMessage(), e);
        }
    }

}
