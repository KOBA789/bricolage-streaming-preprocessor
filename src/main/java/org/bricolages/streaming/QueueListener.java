package org.bricolages.streaming;

import org.bricolages.streaming.event.*;
import org.bricolages.streaming.exception.ApplicationAbort;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.receiver.LocationParser;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3IOException;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.s3.S3ObjectMetadata;
import org.bricolages.streaming.stream.Activity;
import org.bricolages.streaming.stream.ActivityRepository;
import org.bricolages.streaming.stream.DataSinkHandle;
import org.bricolages.streaming.stream.Transformer;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedWriter;
import java.io.IOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class QueueListener implements EventHandlers {
    final EventQueue eventQueue;
    final S3Agent s3;
    final Transformer transformer;
    final LocationParser locationParser;
    final LogQueue logQueue;
    
    @Autowired
    ActivityRepository activityRepos;

    public void run() throws IOException {
        log.info("server started");
        trapSignals();
        try {
            while (!isTerminating()) {
                // FIXME: insert sleep on empty result
                try {
                    handleEvents();
                    eventQueue.flushDelete();
                }
                catch (SQSException ex) {
                    safeSleep(5);
                }
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        eventQueue.flushDeleteForce();
        log.info("application is gracefully shut down");
    }

    public void runOnce() throws Exception {
        trapSignals();
        try {
            while (!isTerminating()) {
                val empty = handleEvents();
                if (empty) break;
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        eventQueue.flushDeleteForce();
    }

    boolean handleEvents() {
        boolean empty = true;
        for (val event : eventQueue.poll()) {
            log.debug("processing message: {}", event.getMessageBody());
            event.callHandler(this);
            empty = false;
        }
        return empty;
    }

    @Override
    public void handleS3Event(S3Event event) {
        log.debug("handling URL: {}", event.getLocation().toString());
        LocationParser.PacketMetadata metadata;
        try {
            metadata = locationParser.parse(event.getLocation());
        } catch (ConfigError err) {
            throw new RuntimeException(err);
        }
        val packet = new S3Packet(s3, metadata.streamName, event.getLocation());
        val sinkHandle = new S3SinkHandler(metadata.destLocation());

        Activity activity = new Activity(event.getLocation().toString(), metadata.destLocation().toString());
        Transformer.Result result = transformer.processWithLogging(packet, sinkHandle, activity);

        switch (result.status) {
            case Processed:
            if (!event.doesNotDispatch()) {
                activity.dispatched();
                activityRepos.save(activity);
                 // FIXME:
                logQueue.send(new FakeS3Event(sinkHandle.destinationMetadata));
            }
            eventQueue.delete(event);
            break;
            case Discarded:
            eventQueue.delete(event);
            break;

            case Failed:
            case Remaining:
            // NOOP
            break;
        }
    }

    @RequiredArgsConstructor
    class S3SinkHandler implements DataSinkHandle {
        private final S3ObjectLocation destination;
        S3ObjectMetadata destinationMetadata; // FIXME:

        public String getLocation() {
            return destination.toString();
        }

        public DataSink open() throws IOException {
            try {
                return new S3Sink(s3.openWriteBuffer(destination, ""));
            } catch(S3IOException ex) {
                throw new IOException(ex);
            }
        }

        @RequiredArgsConstructor
        class S3Sink implements DataSink {
            private final S3Agent.Buffer buf;

            public BufferedWriter getBufferedWriter() {
                return buf.getBufferedWriter();
            }

            public void close() throws IOException {
                try {
                    destinationMetadata = buf.commit(); // FIXME:
                } catch (S3IOException ex) {
                    throw new IOException(ex);
                }
            }
        }
    }

    @Override
    public void handleUnknownEvent(UnknownEvent event) {
        // FIXME: notify?
        log.warn("unknown message: {}", event.getMessageBody());
        eventQueue.deleteAsync(event);
    }

    @Override
    public void handleShutdownEvent(ShutdownEvent event) {
        // Use sync delete to avoid duplicated shutdown
        eventQueue.delete(event);
        initiateShutdown();
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
}
