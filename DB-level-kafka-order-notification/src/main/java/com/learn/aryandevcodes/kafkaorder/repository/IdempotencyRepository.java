package com.learn.aryandevcodes.kafkaorder.repository;

import com.learn.aryandevcodes.kafkaorder.model.IdempotencyRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IdempotencyRepository extends JpaRepository<IdempotencyRecord, Long> {

    Optional<IdempotencyRecord> findByClientRequestId(String clientRequestId);
}
