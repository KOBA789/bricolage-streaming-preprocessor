package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("string")
@MultilineDescription({
    "Text",
    "This provides a shorthand such as `!string [bytes]`",
})
@NoArgsConstructor
public class StringDomain implements ColumnParametersEntry {
    @Getter
    @MultilineDescription("Declares max byte length")
    private Integer bytes;

    public String getType() {
        return String.format("varchar(%d)", bytes);
    }
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list; // empty list
    }

    @JsonCreator
    public StringDomain(String bytes) {
        this.bytes = Integer.valueOf(bytes);
    }

    public void applyDefault(DomainDefaultValues defaultValues) {
        val defaultValue = defaultValues.getString();
        if (defaultValue == null) { return; }
        this.bytes = this.bytes == null ? defaultValue.bytes : this.bytes;
    }
}
