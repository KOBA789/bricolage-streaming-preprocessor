package org.bricolages.streaming;

import org.bricolages.streaming.preprocess.ObjectMapper;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.bricolages.streaming.exception.ConfigError;
import org.yaml.snakeyaml.Yaml;
import lombok.*;
import java.util.List;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

@Component
@ConfigurationProperties(prefix = "bricolage")
public class Config {
    @Getter
    @Setter
    private EventQueueEntry eventQueue;
    @Getter
    @Setter
    private LogQueueEntry logQueue;
    @Getter
    @Setter
    private List<ObjectMapper.Entry> mapping;

    @Getter
    @Setter
    static class EventQueue {
        private String url;
        private int visibilityTimeout;
        private int maxNumberOfMessages;
        private int waitTimeSeconds;
    }

    class EventQueueEntry {
        public final String url;
        public final int visibilityTimeout;
        public final int maxNumberOfMessages;
        public final int waitTimeSeconds;

        public EventQueueEntry(EventQueue eq) {
            this.url = eq.getUrl();
            this.visibilityTimeout = eq.getVisibilityTimeout();
            this.maxNumberOfMessages = eq.getMaxNumberOfMessages();
            this.waitTimeSeconds = eq.getWaitTimeSeconds();
        }
    }

    @Getter
    @Setter
    static class LogQueue {
        private String url;
    }

    class LogQueueEntry {
        public final String url;

        public LogQueueEntry(LogQueue sq) {
            this.url = sq.getUrl();
        }
    }
}
