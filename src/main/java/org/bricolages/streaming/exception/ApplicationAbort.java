package org.bricolages.streaming.exception;

import org.bricolages.streaming.exception.ApplicationError;

public class ApplicationAbort extends ApplicationError {
    private static final long serialVersionUID = 1L;
    public ApplicationAbort(String message) {
        super(message);
    }

    public ApplicationAbort(Exception cause) {
        super(cause);
    }
}
