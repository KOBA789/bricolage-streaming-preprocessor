package org.bricolages.streaming;

import org.bricolages.streaming.exception.ConfigError;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.*;

@Component
@ConfigurationProperties(prefix = "bricolage")
public class Config {
    @Getter
    private final EventQueue eventQueue = new EventQueue();
    @Getter
    private final LogQueue logQueue = new LogQueue();
    @Getter
    private List<MappingEntry> mappings = new ArrayList<>();

    // Setters are required by Spring DI
    @Setter
    static class EventQueue {
        public String url;
        public int visibilityTimeout;
        public int maxNumberOfMessages;
        public int waitTimeSeconds;
    }

    // Setters are required by Spring DI
    @Setter
    static class LogQueue {
        public String url;
    }

    // It seems that it must be public for Spring DI
    @NoArgsConstructor
    public static class MappingEntry {
        @Setter public String srcUrlPattern;
        @Setter public String streamName;
        @Setter public String destBucket;
        @Setter public String streamPrefix;
        @Setter public String objectPrefix;
        @Setter public String objectName;

        public MappingEntry(String srcUrlPattern, String streamName, String destBucket, String streamPrefix, String objectPrefix, String objectName) {
            this.srcUrlPattern = srcUrlPattern;
            this.streamName = streamName;
            this.destBucket = destBucket;
            this.streamPrefix = streamPrefix;
            this.objectPrefix = objectPrefix;
            this.objectName = objectName;
        }

        Pattern pat = null;

        public Pattern sourcePattern() {
            if (pat != null) return pat;
            pat = Pattern.compile("^" + srcUrlPattern + "$");
            return pat;
        }
    }

    private void checkMappings() throws ConfigError {
        for (Config.MappingEntry ent : mappings) {
            try {
                ent.sourcePattern();
            }
            catch (PatternSyntaxException ex) {
                throw new ConfigError("source pattern syntax error: " + ent.srcUrlPattern);
            }
        }
    }

    public void check() throws ConfigError {
        this.checkMappings();
    }
}
