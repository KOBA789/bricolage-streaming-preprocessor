package org.bricolages.streaming.receiver;

import org.bricolages.streaming.Config;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.s3.S3ObjectLocation;

import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class LocationParser {
    final List<Config.MappingEntry> entries;

    public PacketMetadata parse(S3ObjectLocation src) throws ConfigError {
        for (Config.MappingEntry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src.urlString());
            if (m.matches()) {
                return new PacketMetadata(
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

    String safeSubst(String template, Matcher m) throws ConfigError {
        try {
            return m.replaceFirst(template);
        }
        catch (IndexOutOfBoundsException ex) {
            throw new ConfigError("bad replacement: " + template);
        }
    }

    @RequiredArgsConstructor
    public static final class PacketMetadata {
        public final String streamName; // metadata (recognize)
        public final String destBucket; // metadata (recognize)

        public final String streamPrefix; // destination (route)
        public final String objectPrefix; // destination (route)
        public final String objectName;   // destination (route)


        // これがよくない
        public S3ObjectLocation destLocation() {
            return new S3ObjectLocation(destBucket, Paths.get(streamPrefix, objectPrefix, objectName).toString());
        }
    }
}
