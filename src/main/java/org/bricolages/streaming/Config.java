package org.bricolages.streaming;

import org.bricolages.streaming.preprocess.StreamRouter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "bricolage")
public class Config {
    @Getter
    private final EventQueue eventQueue = new EventQueue();
    @Getter
    private final LogQueue logQueue = new LogQueue();
    @Getter
    private List<StreamRouter.Entry> mappings = new ArrayList<>();

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
