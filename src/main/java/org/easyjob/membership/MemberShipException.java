package org.easyjob.membership;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MemberShipException extends RuntimeException {

    private static final long serialVersionUID = -6417179023052012152L;

    public MemberShipException(final String errorMessage, final Exception cause) {
        super(errorMessage, cause);
    }

    public MemberShipException(final Exception cause) {
        super(cause);
    }

}
