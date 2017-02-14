package org.bricolages.streaming.stream;

import java.io.BufferedReader;
import java.io.IOException;

public interface DataPacket {
    public BufferedReader open() throws IOException;

    public String getSourceName(); // for logging or debugging
    
    public String getStreamName();
}