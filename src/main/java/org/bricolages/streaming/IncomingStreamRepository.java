package org.bricolages.streaming;

import org.bricolages.streaming.exception.ApplicationError;
import org.bricolages.streaming.vo.TableId;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface IncomingStreamRepository extends JpaRepository<IncomingStream, Long> {
    List<IncomingStream> findByName(String name);

    default IncomingStream findStream(TableId id) {
        val list = findByName(id.toString());
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + id);
        }
        return list.get(0);
    }
}
