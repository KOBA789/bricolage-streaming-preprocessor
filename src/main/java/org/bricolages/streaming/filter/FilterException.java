package org.bricolages.streaming.filter;
import org.bricolages.streaming.exception.ApplicationException;

public class FilterException extends ApplicationException {
    private static final long serialVersionUID = 1L;
    FilterException(String message) {
        super(message);
    }

    FilterException(Exception cause) {
        super(cause);
    }
}
