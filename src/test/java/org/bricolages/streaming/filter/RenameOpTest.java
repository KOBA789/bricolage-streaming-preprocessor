package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RenameOpTest extends OpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition(stream("schema.table"), "rename", "b", "{\"to\":\"b_renamed\"}");
        val op = (RenameOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"c\":3,\"b_renamed\":2}", out.serialize());
    }
}
