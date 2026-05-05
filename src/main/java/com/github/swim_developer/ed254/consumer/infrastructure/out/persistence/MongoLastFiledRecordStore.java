package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence;

import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.LastFiledRecordStateDocument;
import io.quarkus.mongodb.panache.PanacheMongoRepository;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Optional;

@ApplicationScoped
public class MongoLastFiledRecordStore implements PanacheMongoRepository<LastFiledRecordStateDocument> {

    public Optional<LastFiledRecordStateDocument> findByQueueName(String queueName) {
        return find("queueName", queueName).firstResultOptional();
    }

    public long deleteByQueueName(String queueName) {
        return delete("queueName", queueName);
    }
}
