package org.bricolages.streaming;

import java.util.List;

import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

/*
 * NOT USED YET
 */

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_tables") // -> strload_streams
class TargetTable {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="table_id")
    @Getter
    Long id;

    @Column(name="schema_name")
    String schemaName;

    @Column(name="table_name")
    String tableName;

    @Column(name="load_batch_size")
    @Getter
    Long loadBatchSize;

    @Column(name="load_interval")
    @Getter
    Long loadInterval;

    @Column(name="disabled")
    Boolean disabled;

    //@OneToMany(mappedBy="table")
    //List<PacketStream> streams;

    public boolean isDisabled() {
        return this.disabled.booleanValue();
    }
}
