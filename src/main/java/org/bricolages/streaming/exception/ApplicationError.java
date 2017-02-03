package org.bricolages.streaming.exception;

public class ApplicationError extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public ApplicationError(String message) {
        super(message);
    }

    public ApplicationError(Exception cause) {
        super(cause);
    }
}
