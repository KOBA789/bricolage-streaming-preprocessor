package org.bricolages.streaming.stream;

import java.io.BufferedWriter;
import java.io.IOException;

public interface DataSinkHandle {
    public DataSink open() throws IOException;
    public String getLocation(); // for logging or debugging

    public static interface DataSink extends AutoCloseable {
        public BufferedWriter getBufferedWriter();
        public void close() throws IOException;
    }
}
