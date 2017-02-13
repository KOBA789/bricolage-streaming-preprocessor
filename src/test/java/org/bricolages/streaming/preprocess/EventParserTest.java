package org.bricolages.streaming.preprocess;

import org.bricolages.streaming.Config;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.s3.S3UrlParseException;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class EventParserTest {
    EventParser newParser(Config.MappingEntry entries) {
        return new EventParser(Arrays.asList(entries));
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void parse() throws Exception {
        val entry = new Config.MappingEntry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "dest-bucket", "dest-prefix/$1", "", "$2");
        val parser = newParser(entry);
        val result = parser.parse(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.destLocation());
        assertEquals("schema.table", result.streamName);
        assertNull(parser.parse(loc("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz")));
    }

    /*
    @Test(expected=ConfigError.class)
    public void map_baddest() throws Exception {
        val map = newParser(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$3", "$3"));
        map.check();
        map.map(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
    }
    
    @Test(expected=ConfigError.class)
    public void map_badregex() throws Exception {
        val map = newParser(entry("****", "$1/$2", "$3"));
        map.check();
    }
    */
}
