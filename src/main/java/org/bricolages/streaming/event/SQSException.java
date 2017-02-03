package org.bricolages.streaming.event;
import org.bricolages.streaming.exception.ApplicationError;

public class SQSException extends ApplicationError {
    private static final long serialVersionUID = 1L;
    SQSException(String message) {
        super(message);
    }

    SQSException(Exception cause) {
        super(cause);
    }
}
