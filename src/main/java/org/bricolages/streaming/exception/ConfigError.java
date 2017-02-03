package org.bricolages.streaming.exception;

import org.bricolages.streaming.exception.ApplicationError;

public class ConfigError extends ApplicationError {
    private static final long serialVersionUID = 1L;
    public ConfigError(String message) {
        super(message);
    }

    public ConfigError(Exception cause) {
        super(cause);
    }
}
