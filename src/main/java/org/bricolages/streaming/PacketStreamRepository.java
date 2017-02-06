package org.bricolages.streaming;

import org.bricolages.streaming.exception.ApplicationError;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface PacketStreamRepository extends JpaRepository<PacketStream, Long> {
    List<PacketStream> findByTableId(String tableId);

    default PacketStream findParams(String id) {
        val list = findByTableId(id);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + id);
        }
        return list.get(0);
    }
}
