package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence;

import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.ArrivalEventDocument;
import io.quarkus.mongodb.panache.common.ProjectionFor;

@ProjectionFor(ArrivalEventDocument.class)
public class ArrivalEventHashProjection {
    private String subscriptionId;
    private String contentHash;

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public String getContentHash() {
        return contentHash;
    }

    public void setContentHash(String contentHash) {
        this.contentHash = contentHash;
    }
}
