package org.bricolages.streaming.s3;
import org.bricolages.streaming.exception.ConfigError;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectMapperTest {
    ObjectMapper newMapper(ObjectMapper.Entry... entries) {
        return new ObjectMapper(Arrays.asList(entries));
    }

    ObjectMapper.Entry entry(String src, String dest, String table) {
        return new ObjectMapper.Entry(src, dest, table);
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void map() throws Exception {
        val map = newMapper(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "s3://dest-bucket/dest-prefix/$1/$2", "$1"));
        map.check();
        val result = map.map(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getTableId());
        assertNull(map.map(loc("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz")));
    }

    @Test(expected=ConfigError.class)
    public void map_baddest() throws Exception {
        val map = newMapper(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$3", "$3"));
        map.check();
        map.map(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
    }

    @Test(expected=ConfigError.class)
    public void map_badregex() throws Exception {
        val map = newMapper(entry("****", "$1/$2", "$3"));
        map.check();
    }
}
