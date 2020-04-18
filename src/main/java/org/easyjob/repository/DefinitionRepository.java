package org.easyjob.repository;

import org.easyjob.JobDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface DefinitionRepository {

    void save(JobDefinition jobDefinition);

    void update(String jobId, JobDefinition jobDefinition);

    void remove(String jobId);

    @NotNull JobDefinition get(String jobId);

    boolean contain(String jobId);

    void addListener(DefinitionChangedListener listener);

    List<JobDefinition> list();

    List<String> listId();

    void close();
}
