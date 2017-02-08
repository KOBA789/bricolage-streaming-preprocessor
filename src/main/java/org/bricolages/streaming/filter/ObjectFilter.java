package org.bricolages.streaming.filter;
import java.util.List;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectFilter {
    final List<Op> operators;

    public ObjectFilter(List<Op> operators) {
        this.operators = operators;
    }

    static public final class Stats {
        public int inputRows;
        public int outputRows;
        public int errorRows;
    }

    public Stats apply(BufferedReader r, BufferedWriter w, String sourceName) throws IOException {
        final Stats stats = new Stats();
        final PrintWriter out = new PrintWriter(w);
        r.lines().forEach((line) -> {
            if (line.trim().isEmpty()) return;  // should not count blank line
            stats.inputRows++;
            try {
                String outStr = applyString(line);
                if (outStr != null) {
                    out.println(outStr);
                    stats.outputRows++;
                }
            }
            catch (JSONException ex) {
                log.debug("JSON parse error: {}:{}: {}", sourceName, stats.inputRows, ex.getMessage());
                stats.errorRows++;
            }
        });
        return stats;
    }

    public String applyString(String json) throws JSONException {
        Record record = Record.parse(json);
        if (record == null) return null;
        for (Op op : operators) {
            record = op.apply(record);
            if (record == null) return null;
        }
        return record.serialize();
    }
}
