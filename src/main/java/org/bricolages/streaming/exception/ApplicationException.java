package org.bricolages.streaming.exception;

public class ApplicationException extends Exception {
    private static final long serialVersionUID = 1L;
    public ApplicationException(String message) {
        super(message);
    }

    public ApplicationException(Exception cause) {
        super(cause);
    }
}
