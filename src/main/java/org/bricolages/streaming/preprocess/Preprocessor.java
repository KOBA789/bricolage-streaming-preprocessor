package org.bricolages.streaming.preprocess;

import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.exception.ApplicationAbort;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.s3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
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
    final ObjectMapper mapper;
    final ObjectFilterFactory filterFactory;

    public void processObject(S3ObjectLocation location, boolean doesDispatch) {
        val mapResult = mapper.map(location);
        if (mapResult == null) {
            log.warn("could not detect a stream: {}", location);
            return;
        }
        processPacket(location, mapResult, doesDispatch);
    }

    public void processPacket(S3ObjectLocation location, ObjectMapper.Result mapResult, boolean doesDispatch) {
        PacketStream stream = streamRepos.findStream(mapResult.streamName);
        val filter = filterFactory.load(stream);

        if (stream.isDisabled()) {
            // TODO:
            //stream.defer(packet);
            return;
        }
        if (stream.isDiscarded()) {
            log.debug("discard event: {}", location);
            return;
        }
        Result result = process(location, mapResult);
        if (result != null && doesDispatch) {
            writeDispatchInfo(mapResult, result);
        }
    }

    void writeDispatchInfo(ObjectMapper.Result mapResult, Result result) {
        // FIXME
    }

    public Result process(S3ObjectLocation location, ObjectMapper.Result mapResult) {
        //Activity activity = new Activity(packet);
        //activityRepos.save(activity);
        try {
            ObjectFilter filter = filterFactory.load(streamRepos.findStream(mapResult.streamName));
            Result result = applyFilter(filter, location, mapResult.destLocation(), mapResult.streamName);
            log.debug("src: {}, dest: {}, in: {}, out: {}", location, mapResult.destLocation(), result.stats.inputRows, result.stats.outputRows);
            //activity.succeeded();
            //activityRepos.save(activity);
            return result;
        }
        catch (S3IOException | IOException ex) {
            log.error("src: {}, error: {}", location, ex.getMessage());
            //activity.failed(ex.getMessage());
            //activityRepos.save(activity);
            return null;
        }
        catch (ConfigError ex) {
            log.error("src: {}, error: {}", location, ex.getMessage());
            //activity.error(ex.getMessage());
            //activityRepos.save(activity);
            return null;
        }
    }

    Thread mainThread;
    boolean isTerminating = false;
    
    void trapSignals() {
        mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                initiateShutdown();
                waitMainThread();
            }
        });
    }

    void initiateShutdown() {
        log.info("initiate shutdown; mainThread={}", mainThread);
        this.isTerminating = true;
        if (mainThread != null) {
            mainThread.interrupt();
        }
    }

    boolean isTerminating() {
        if (isTerminating) return true;
        if (mainThread.isInterrupted()) {
            this.isTerminating = true;
            return true;
        }
        else {
            return false;
        }
    }

    void waitMainThread() {
        if (mainThread == null) return;
        try {
            log.info("waiting main thread...");
            mainThread.join();
        }
        catch (InterruptedException ex) {
            // ignore
        }
    }

    void safeSleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        }
        catch (InterruptedException ex) {
            this.isTerminating = true;
        }
    }

    @Autowired
    FilterResultRepository repos;

    @Autowired
    PacketStreamRepository streamRepos;

    @NoArgsConstructor
    static final class Result {
        ObjectFilter.Stats stats;
        S3ObjectMetadata metadata;
    }

    Result applyFilter(ObjectFilter filter, S3ObjectLocation src, S3ObjectLocation dest, String streamName) throws S3IOException, IOException {
        Result result = new Result();
        try (S3Agent.Buffer buf = s3.openWriteBuffer(dest, streamName)) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, buf.getBufferedWriter(), src.toString());
            }
            result.metadata = buf.commit();
            return result;
        }
    }

    public void processOnly(S3ObjectLocation loc, BufferedWriter out) throws S3IOException, IOException {
        val mapResult = mapper.map(loc);
        PacketStream stream = streamRepos.findStream(mapResult.streamName);
        val filter = filterFactory.load(stream);
        
        try (BufferedReader r = s3.openBufferedReader(loc)) {
            val stats = filter.apply(r, out, loc.toString());
            log.debug("src: {}, dest: {}, in: {}, out: {}", loc, mapResult.destLocation(), stats.inputRows, stats.outputRows);
        }
    }
}
