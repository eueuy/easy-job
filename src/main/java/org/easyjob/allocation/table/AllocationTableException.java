package org.easyjob.allocation.table;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AllocationTableException extends RuntimeException {

    private static final long serialVersionUID = -6417179023552012152L;

    public AllocationTableException(final String errorMessage, final Exception cause) {
        super(errorMessage, cause);
    }

    public AllocationTableException(final Exception cause) {
        super(cause);
    }

}
