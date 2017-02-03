package org.bricolages.streaming.filter;
import org.bricolages.streaming.exception.ApplicationException;

public class JSONException extends ApplicationException {
    private static final long serialVersionUID = 1L;
    JSONException(String message) {
        super(message);
    }

    JSONException(Exception cause) {
        super(cause);
    }
}
