package org.easyjob.repository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DefinitionRepositoryException extends RuntimeException {

    private static final long serialVersionUID = -6417179023552012152L;

    public DefinitionRepositoryException(final String errorMessage, final Exception cause) {
        super(errorMessage, cause);
    }

    public DefinitionRepositoryException(final Exception cause) {
        super(cause);
    }

}
