package org.bricolages.streaming.filter;

import org.bricolages.streaming.exception.ConfigError;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    OperatorDefinitionRepository repos;

    @Autowired
    OpBuilder builder;

    public ObjectFilter load(List<OperatorDefinition> operatorDefs) throws ConfigError {
        List<Op> ops = new ArrayList<>(operatorDefs.size());
        for (OperatorDefinition def: operatorDefs) {
            Op op = builder.build(def);
            log.debug("operator stacked: {}", op);
            ops.add(op);
        }
        return new ObjectFilter(ops);
    }
}
