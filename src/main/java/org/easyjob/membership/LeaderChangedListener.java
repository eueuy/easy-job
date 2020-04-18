package org.easyjob.membership;

import java.util.List;

public interface LeaderChangedListener {

    void gotLeadership(List<String> nodeList);

}
