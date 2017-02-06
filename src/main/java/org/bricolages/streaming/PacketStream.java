package org.bricolages.streaming;

import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="preproc_tables") // -> strload_streams
public class PacketStream {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    //@Column(name="stream_id")
    @Getter
    long id;
/*
    @Getter
    @Column(name="stream_name", nullable=false)
    String name;

    @Column(name="stream_prefix", nullable=false)
    String prefix;
*/
    @Column(name="table_id")
    String tableId;

    @Column(name="disabled")
    @Getter
    boolean disabled;

    @Column(name="discard")
    boolean discard;
/*
    public PacketStream(String name, String prefix) {
        this.name = name;
        this.prefix = prefix;
        this.disabled = true;
    }
*/
    public boolean isDiscarded() {
        return this.discard;
    }
}
