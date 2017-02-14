package org.bricolages.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3IOException;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.stream.DataPacket;
import lombok.*;

@RequiredArgsConstructor
public class S3Packet implements DataPacket {
    private final S3Agent s3;
    @Getter
    private final String streamName;

    private final S3ObjectLocation sourceLocation;

    public String getSourceName() {
        return sourceLocation.toString();
    }

    public BufferedReader open() throws IOException {
        try {
            return s3.openBufferedReader(sourceLocation);
        } catch (S3IOException ex) {
            throw new IOException(ex);
        }
    }
}
