package org.bricolages.streaming.stream;

import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_stream_bundles")
public class StreamBundle {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="stream_bundle_id")
    @Getter
    long id;

    @ManyToOne
    @JoinColumn(name="stream_id")
    @Getter
    PacketStream stream;

    @Column(name="stream_prefix", nullable=false)
    @Getter
    String prefix;
}
