package org.bricolages.streaming.stream;

import java.util.List;
import javax.persistence.*;
import org.bricolages.streaming.filter.OperatorDefinition;


import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_streams")
public class PacketStream {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="stream_id")
    @Getter
    long id;

    @Column(name="stream_name", nullable=false)
    @Getter
    String name;

    @Column(name="stream_prefix", nullable=false)
    @Getter
    String prefix;

    //@ManyToOne
    //@JoinColumn(name="table_id")
    //@Getter
    //TargetTable table;
    
    @Column(name="disabled")
    @Getter
    boolean disabled;

    @Column(name="discard")
    boolean discard;

    @OneToMany(mappedBy="stream", fetch=FetchType.EAGER)
    @OrderBy("application_order ASC")
    @Getter
    List<OperatorDefinition> operators;

    public PacketStream(String name, String prefix) {
        this.name = name;
        this.prefix = prefix;
        this.disabled = true;
    }

    public boolean isDiscarded() {
        return this.discard;
    }
}
