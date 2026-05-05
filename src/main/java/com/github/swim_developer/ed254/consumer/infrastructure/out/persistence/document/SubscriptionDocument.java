package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document;

import com.github.swim_developer.ed254.consumer.domain.model.DestinationAerodrome;
import com.github.swim_developer.ed254.consumer.domain.model.FlightSelector;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.domain.model.SupplementaryData;
import com.github.swim_developer.framework.persistence.mongodb.MongoSubscriptionDocumentPort;
import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.List;

@MongoEntity(collection = "ed254_subscriptions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SubscriptionDocument implements MongoSubscriptionDocumentPort {

    private ObjectId id;
    private String subscriptionId;
    private String queueName;
    private String subscriptionStatus;
    private List<DestinationAerodrome> destinationAerodrome;
    private List<String> pointName;
    private List<FlightSelector> flightSelector;
    private SupplementaryData supplementaryData;
    private String description;
    private String type;
    private String configHash;
    private Instant subscriptionEnd;
    private String providerName;
    private String providerId;
    private String heartbeatQueue;

    public static SubscriptionDocument fromDomain(Subscription subscription) {
        SubscriptionDocument doc = new SubscriptionDocument();
        doc.setId(subscription.getId() != null ? new ObjectId(subscription.getId()) : null);
        doc.setSubscriptionId(subscription.getSubscriptionId());
        doc.setQueueName(subscription.getQueueName());
        doc.setSubscriptionStatus(subscription.getSubscriptionStatus());
        doc.setDescription(subscription.getDescription());
        doc.setType(subscription.getType());
        doc.setConfigHash(subscription.getConfigHash());
        doc.setSubscriptionEnd(subscription.getSubscriptionEnd());
        doc.setProviderName(subscription.getProviderName());
        doc.setProviderId(subscription.getProviderId());
        doc.setHeartbeatQueue(subscription.getHeartbeatQueue());
        return doc;
    }

    public Subscription toDomain() {
        Subscription sub = new Subscription();
        sub.setId(id != null ? id.toHexString() : null);
        sub.setSubscriptionId(subscriptionId);
        sub.setQueueName(queueName);
        sub.setSubscriptionStatus(subscriptionStatus);
        sub.setDestinationAerodrome(destinationAerodrome);
        sub.setPointName(pointName);
        sub.setFlightSelector(flightSelector);
        sub.setAnySupplementaryData(supplementaryData != null && supplementaryData.anyRequested());
        sub.setDescription(description);
        sub.setType(type);
        sub.setConfigHash(configHash);
        sub.setSubscriptionEnd(subscriptionEnd);
        sub.setProviderName(providerName);
        sub.setProviderId(providerId);
        sub.setHeartbeatQueue(heartbeatQueue);
        return sub;
    }
}
