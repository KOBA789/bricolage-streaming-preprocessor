package org.bricolages.streaming.preprocess;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.s3.S3ObjectLocation;
import java.util.Objects;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class ObjectMapper {
    final List<Entry> entries;

    void check() throws ConfigError {
        for (Entry ent : entries) {
            try {
                ent.sourcePattern();
            }
            catch (PatternSyntaxException ex) {
                throw new ConfigError("source pattern syntax error: " + ent.srcUrlPattern);
            }
        }
    }

    public Result map(S3ObjectLocation src) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src.urlString());
            if (m.matches()) {
                return new Result(
                    safeSubst(ent.streamName, m),
                    ent.destBucket,
                    safeSubst(ent.streamPrefix, m),
                    safeSubst(ent.objectPrefix, m),
                    safeSubst(ent.objectName, m)
                );
            }
        }
        // FIXME: error??
        log.error("unknown S3 object URL: {}", src);
        return null;
    }

    String safeSubst(String template, Matcher m) {
        try {
            return m.replaceFirst(template);
        }
        catch (IndexOutOfBoundsException ex) {
            throw new ConfigError("bad replacement: " + template);
        }
    }

    @NoArgsConstructor
    public static final class Entry {
        public String srcUrlPattern;
        public String streamName;
        public String destBucket;
        public String streamPrefix;
        public String objectPrefix;
        public String objectName;

        Entry(String srcUrlPattern, String streamName, String destBucket, String streamPrefix, String objectPrefix, String objectName) {
            this.srcUrlPattern = srcUrlPattern;
            this.streamName = streamName;
            this.destBucket = destBucket;
            this.streamPrefix = streamPrefix;
            this.objectPrefix = objectPrefix;
            this.objectName = objectName;
        }

        Pattern pat = null;

        Pattern sourcePattern() {
            if (pat != null) return pat;
            pat = Pattern.compile("^" + srcUrlPattern + "$");
            return pat;
        }
    }

    @RequiredArgsConstructor
    public static final class Result {
        public final String streamName;
        public final String destBucket;
        public final String streamPrefix;
        public final String objectPrefix;
        public final String objectName;

        public S3ObjectLocation destLocation() {
            return new S3ObjectLocation(destBucket, streamPrefix + "/" + objectPrefix + "/" + objectName);
        }
    }
}
