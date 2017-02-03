package org.bricolages.streaming.event;
import org.bricolages.streaming.exception.ApplicationException;

public class MessageParseException extends ApplicationException {
    private static final long serialVersionUID = 1L;

    MessageParseException(String message) {
        super(message);
    }

    MessageParseException(Exception cause) {
        super(cause);
    }
}
