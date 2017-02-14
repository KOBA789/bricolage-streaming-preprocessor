package org.bricolages.streaming.stream;

import org.springframework.data.jpa.repository.JpaRepository;

import lombok.*;

public interface StreamBundleRepository extends JpaRepository<StreamBundle, Long> {
}
