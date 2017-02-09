package org.bricolages.streaming;

import org.bricolages.streaming.preprocess.ObjectMapper;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.bricolages.streaming.exception.ConfigError;
import org.yaml.snakeyaml.Yaml;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

@Component
@ConfigurationProperties(prefix = "bricolage")
public class Config {
    @Getter
    private final EventQueue eventQueue = new EventQueue();
    @Getter
    private final LogQueue logQueue = new LogQueue();
    @Getter
    private List<ObjectMapper.Entry> mappings = new ArrayList<>();

    @Getter
    @Setter
    static class EventQueue {
        public String url;
        public int visibilityTimeout;
        public int maxNumberOfMessages;
        public int waitTimeSeconds;
    }

    @Getter
    @Setter
    static class LogQueue {
        public String url;
    }

    class LogQueueEntry {
        public final String url;

        public LogQueueEntry(LogQueue sq) {
            this.url = sq.getUrl();
        }
    }
}
