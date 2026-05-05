package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.domain.model.SwimOutboxEvent;
import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.types.ObjectId;

import java.time.Instant;

@MongoEntity(collection = "ed254_arrival_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ArrivalEventDocument implements SwimOutboxEvent {

    private ObjectId id;
    private String subscriptionId;
    private String messageId;
    private String aerodromeDesignator;
    private String messageType;
    private String rawPayload;
    private boolean compressed;
    private Instant publicationTime;
    private Instant creationTime;
    private boolean firstMessageAfterServiceOutage;
    private int sequenceEntriesCount;
    private String contentHash;
    @BsonProperty("kafkaStatus")
    private OutboxDeliveryStatus deliveryStatus;
    @BsonProperty("retryCount")
    private int dispatchRetryCount;
    private Instant receivedAt;
    @BsonProperty("sentToKafkaAt")
    private Instant dispatchedAt;

    @Override
    public int getOutboxRetryCount() {
        return dispatchRetryCount;
    }

    @Override
    public void setOutboxRetryCount(int count) {
        this.dispatchRetryCount = count;
    }

    public ArrivalEvent toDomain() {
        ArrivalEvent event = new ArrivalEvent();
        event.setId(id != null ? id.toHexString() : null);
        event.setSubscriptionId(subscriptionId);
        event.setMessageId(messageId);
        event.setAerodromeDesignator(aerodromeDesignator);
        event.setMessageType(messageType);
        event.setRawPayload(rawPayload);
        event.setCompressed(compressed);
        event.setPublicationTime(publicationTime);
        event.setCreationTime(creationTime);
        event.setFirstMessageAfterServiceOutage(firstMessageAfterServiceOutage);
        event.setSequenceEntriesCount(sequenceEntriesCount);
        event.setContentHash(contentHash);
        event.setDeliveryStatus(deliveryStatus);
        event.setDispatchRetryCount(dispatchRetryCount);
        event.setReceivedAt(receivedAt);
        event.setDispatchedAt(dispatchedAt);
        return event;
    }

    public static ArrivalEventDocument fromDomain(ArrivalEvent event) {
        ArrivalEventDocument doc = new ArrivalEventDocument();
        doc.setId(event.getId() != null ? new ObjectId(event.getId()) : null);
        doc.setSubscriptionId(event.getSubscriptionId());
        doc.setMessageId(event.getMessageId());
        doc.setAerodromeDesignator(event.getAerodromeDesignator());
        doc.setMessageType(event.getMessageType());
        doc.setRawPayload(event.getRawPayload());
        doc.setCompressed(event.isCompressed());
        doc.setPublicationTime(event.getPublicationTime());
        doc.setCreationTime(event.getCreationTime());
        doc.setFirstMessageAfterServiceOutage(event.isFirstMessageAfterServiceOutage());
        doc.setSequenceEntriesCount(event.getSequenceEntriesCount());
        doc.setContentHash(event.getContentHash());
        doc.setDeliveryStatus(event.getDeliveryStatus());
        doc.setDispatchRetryCount(event.getDispatchRetryCount());
        doc.setReceivedAt(event.getReceivedAt());
        doc.setDispatchedAt(event.getDispatchedAt());
        return doc;
    }
}
