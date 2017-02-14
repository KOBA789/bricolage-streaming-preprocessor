package org.bricolages.streaming.stream;

import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.exception.ConfigError;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.IOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Transformer {
    final ObjectFilterFactory filterFactory;

    @Autowired
    PacketStreamRepository streamRepos;

    @Autowired
    ActivityRepository activityRepos;

    public enum ExitStatus {
        Processed,
        Failed,
        Remaining,
        Discarded,
    }

    @RequiredArgsConstructor
    public static final class Result {
        public final ExitStatus status;
        public final ObjectFilter.Stats stats;

        public static Result processed(ObjectFilter.Stats stats) {
            return new Result(ExitStatus.Processed, stats);
        }

        public static Result failed() {
            return new Result(ExitStatus.Failed, null);
        }

        public static Result remaining() {
            return new Result(ExitStatus.Remaining, null);
        }

        public static Result discarded() {
            return new Result(ExitStatus.Discarded, null);
        }
    }

    public Result processOnly(DataPacket packet, DataSinkHandle sinkHandle) {
        PacketStream stream = streamRepos.findStream(packet.getStreamName());
        try {
            val result = process(packet, sinkHandle, stream);
            return result;
        } catch(ConfigError | IOException ex) {
            log.error("src: {}, error: {}", packet.getSourceName(), ex.getMessage());
            return Result.failed();
        }
    }

    public Result processWithLogging(DataPacket packet, DataSinkHandle sinkHandle, Activity activity) {
        String source = packet.getSourceName();
        PacketStream stream = streamRepos.findStream(packet.getStreamName());
        
        if (stream.isDisabled()) {
            return Result.remaining();
        }
        if (stream.isDiscarded()) {
            log.debug("discard event: {}", source);
            return Result.discarded();
        }

        activityRepos.save(activity);
        try {
            val result = process(packet, sinkHandle, stream);
            activity.succeeded();
            activityRepos.save(activity);
            return result;
        } catch(IOException | ConfigError ex) {
            log.error("src: {}, error: {}", source, ex.getMessage());
            activity.failed(ex.getMessage());
            activityRepos.save(activity);
            return Result.failed();
        }
    }

    public Result process(DataPacket packet, DataSinkHandle sinkHandle, PacketStream stream) throws IOException, ConfigError {
        ObjectFilter filter = filterFactory.load(stream.getOperators());
        return applyFilter(filter, packet, sinkHandle);
    }

    Result applyFilter(ObjectFilter filter, DataPacket packet, DataSinkHandle sinkHandle) throws IOException {
        try (DataSinkHandle.DataSink sink = sinkHandle.open()) {
            try (BufferedReader r = packet.open()) {
                val stats = filter.apply(r, sink.getBufferedWriter(), packet.getSourceName());
                return Result.processed(stats);
            }
        }
    }
}
