package org.bricolages.streaming.preprocess;

import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.s3.*;
import org.bricolages.streaming.stream.PacketStream;
import org.bricolages.streaming.stream.PacketStreamRepository;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor {
    final LogQueue logQueue;
    final S3Agent s3;
    final EventParser parser;
    final ObjectFilterFactory filterFactory;

    @Autowired
    PacketStreamRepository streamRepos;

    @Autowired
    ActivityRepository activityRepos;

    public boolean processObject(S3ObjectLocation location, boolean doesDispatch) {
        val metadata = parser.parse(location);
        if (metadata == null) {
            log.warn("could not detect a stream: {}", location);
            return false;
        }
        return processPacket(location, metadata, doesDispatch);
    }

    public void processOnly(S3ObjectLocation location, BufferedWriter out) throws S3IOException, IOException {
        val metadata = parser.parse(location);
        PacketStream stream = streamRepos.findStream(metadata.streamName);
        val filter = filterFactory.load(stream.getOperators());

        try (BufferedReader r = s3.openBufferedReader(location)) {
            val stats = filter.apply(r, out, location.toString());
            log.debug("src: {}, dest: {}, in: {}, out: {}", location, metadata.destLocation(), stats.inputRows, stats.outputRows);
        }
    }

    boolean processPacket(S3ObjectLocation location, EventParser.PacketMetadata metadata, boolean doesDispatch) {
        PacketStream stream = streamRepos.findStream(metadata.streamName);

        if (stream.isDisabled()) {
            // TODO:
            //stream.defer(packet);
            return false;
        }
        if (stream.isDiscarded()) {
            log.debug("discard event: {}", location);
            return true; // treat as processed to discard event
        }
        Result result = process(location, metadata);
        if (result != null && doesDispatch) {
            writeDispatchInfo(metadata, result);
        }
        return true;
    }

    void writeDispatchInfo(EventParser.PacketMetadata metadata, Result result) {
        // FIXME
    }

    Result process(S3ObjectLocation location, EventParser.PacketMetadata metadata) {
        Activity activity = new Activity(location.toString(), metadata.destLocation().toString());
        activityRepos.save(activity);
        try {
            ObjectFilter filter = filterFactory.load(streamRepos.findStream(metadata.streamName).getOperators());
            Result result = applyFilter(filter, location, metadata.destLocation(), metadata.streamName);
            log.debug("src: {}, dest: {}, in: {}, out: {}", location, metadata.destLocation(), result.stats.inputRows, result.stats.outputRows);
            activity.succeeded();
            activityRepos.save(activity);
            return result;
        }
        catch (S3IOException | IOException ex) {
            log.error("src: {}, error: {}", location, ex.getMessage());
            activity.failed(ex.getMessage());
            activityRepos.save(activity);
            return null;
        }
        catch (ConfigError ex) {
            log.error("src: {}, error: {}", location, ex.getMessage());
            activity.error(ex.getMessage());
            activityRepos.save(activity);
            return null;
        }
    }

    @NoArgsConstructor
    static final class Result {
        ObjectFilter.Stats stats;
        S3ObjectMetadata metadata;
    }

    Result applyFilter(ObjectFilter filter, S3ObjectLocation src, S3ObjectLocation dest, String streamName) throws S3IOException, IOException {
        Result result = new Result();
        try (S3Agent.Buffer buf = s3.openWriteBuffer(dest, streamName)) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                result.stats = filter.apply(r, buf.getBufferedWriter(), src.toString());
            }
            result.metadata = buf.commit();
            return result;
        }
    }
}
