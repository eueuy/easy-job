package org.easyjob;

import org.easyjob.repository.DefinitionRepository;
import org.easyjob.util.UUIDUtil;

import java.util.List;

@SuppressWarnings("unused")
public class JobService {

    private DefinitionRepository definitionRepository;

    public JobService(DefinitionRepository definitionRepository) {
        this.definitionRepository = definitionRepository;
    }

    public boolean isExist(String id) {
        return definitionRepository.contain(id);
    }

    public JobDefinition get(String id) {
        return definitionRepository.get(id);
    }

    public void submit(JobDefinition jobDefinition) {
        if (jobDefinition.getId() == null) {
            jobDefinition.setId(UUIDUtil.generate8Code());
        }
        definitionRepository.save(jobDefinition);
    }

    public void restart(String id) {
        if (!definitionRepository.contain(id)) {
            throw new IllegalArgumentException("job not exist");
        }

        JobDefinition jobDefinition = get(id);
        update(id, jobDefinition);
    }

    public void cancel(String id) {
        definitionRepository.remove(id);
    }

    public void update(String id, JobDefinition jobDefinition) {
        definitionRepository.update(id, jobDefinition);
    }

    public List<String> listId() {
        return definitionRepository.listId();
    }

    public List<JobDefinition> list() {
        return definitionRepository.list();
    }
}
