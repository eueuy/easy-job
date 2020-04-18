package org.easyjob.membership;

import java.util.List;

public interface MembershipChangedListener {

    void join(boolean hasLeadership, boolean isMine, String changedNodeId, List<String> newNodeList);

    void leave(boolean hasLeadership, boolean isMine, String changedNodeId, List<String> newNodeList);

}
