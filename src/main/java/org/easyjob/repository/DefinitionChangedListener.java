package org.easyjob.repository;

import org.easyjob.JobDefinition;

public interface DefinitionChangedListener {

    void created(JobDefinition jobDefinition);

    void removed(JobDefinition jobDefinition);

    void updated(JobDefinition jobDefinition);

}

