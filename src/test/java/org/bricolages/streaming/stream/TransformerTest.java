package org.bricolages.streaming.stream;

import org.bricolages.streaming.Application;
import org.bricolages.streaming.Config;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.s3.S3UrlParseException;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TransformerTest {

    @Test
    public void processObject() throws Exception {
        //val app = new Application();
        //val preprocessor = new Transformer(app.logQueue(), app.s3(), app.eventParser(), app.filterFactory());
        //preprocessor.processObject(new S3ObjectLocation("", key), doesDispatch);
    }
}
