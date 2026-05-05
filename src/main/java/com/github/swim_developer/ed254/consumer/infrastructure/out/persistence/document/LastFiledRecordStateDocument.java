package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.List;

@MongoEntity(collection = "ed254_last_filed_record_state")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LastFiledRecordStateDocument {

    private ObjectId id;
    private String queueName;
    private List<String> arcids;
    private Instant updatedAt;
}
