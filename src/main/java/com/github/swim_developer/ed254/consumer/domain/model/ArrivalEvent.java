package com.github.swim_developer.ed254.consumer.domain.model;

import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.domain.model.SwimOutboxEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ArrivalEvent implements SwimOutboxEvent {

    private String id;
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
    private OutboxDeliveryStatus deliveryStatus;
    private int dispatchRetryCount;
    private Instant receivedAt;
    private Instant dispatchedAt;

    @Override
    public int getOutboxRetryCount() { return dispatchRetryCount; }

    @Override
    public void setOutboxRetryCount(int count) { this.dispatchRetryCount = count; }
}
