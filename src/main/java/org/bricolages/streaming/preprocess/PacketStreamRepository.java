package org.bricolages.streaming.preprocess;

import org.bricolages.streaming.exception.ApplicationError;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface PacketStreamRepository extends JpaRepository<PacketStream, Long> {
    List<PacketStream> findByStreamName(String streamName);

    default PacketStream findStream(String streamName) {
        val list = findByStreamName(streamName);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple streams matched: " + streamName);
        }
        return list.get(0);
    }
}
