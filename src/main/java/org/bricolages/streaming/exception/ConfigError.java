package org.bricolages.streaming.exception;

public class ConfigError extends ApplicationError {
    private static final long serialVersionUID = 1L;
    public ConfigError(String message) {
        super(message);
    }

    public ConfigError(Exception cause) {
        super(cause);
    }
}
