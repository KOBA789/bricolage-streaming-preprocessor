package org.bricolages.streaming.filter;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.bricolages.streaming.exception.ConfigError;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.*;

@Slf4j
public class OpBuilder {
    @Autowired
    SequencialNumberRepository sequencialNumberRepository;

    public OpBuilder() {
        registerAll();
    }

    void registerAll() {
        IntOp.register(this);
        BigIntOp.register(this);
        TextOp.register(this);
        TimeZoneOp.register(this);
        UnixTimeOp.register(this);
        DeleteNullsOp.register(this);
        AggregateOp.register(this);
        DeleteOp.register(this);
        RenameOp.register(this);
        CollectRestOp.register(this);
        RejectOp.register(this);
        SequenceOp.register(this);
        DupOp.register(this);
        FloatOp.register(this);
    }

    private Map<String, Function<OperatorDefinition, Op>> builders = new HashMap<String, Function<OperatorDefinition, Op>>();
    public void registerOperator(String id, Function<OperatorDefinition, Op> builder) {
        log.debug("new operator builder registered: '{}' -> {}", id, builder);
        builders.put(id, builder);
    }

    final public Op build(OperatorDefinition def) throws ConfigError {
        val builder = builders.get(def.operatorId);
        if (builder == null) {
            throw new ConfigError("unknown operator ID: " + def.operatorId);
        }
        return builder.apply(def);
    }
}