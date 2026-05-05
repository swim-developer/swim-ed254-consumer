package com.github.swim_developer.ed254.consumer.application.usecase;

import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.ed254.consumer.domain.model.FilterDimension;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.application.port.in.ManageSubscriptionPort;
import com.github.swim_developer.ed254.consumer.domain.port.out.RemoteSubscriptionManagerPort;
import com.github.swim_developer.ed254.consumer.domain.port.out.SubscriptionStore;
import com.github.swim_developer.ed254.consumer.application.port.out.Ed254SequenceGapCachePort;
import com.github.swim_developer.framework.application.port.out.SwimProviderConfigPort;
import com.github.swim_developer.framework.application.port.out.SwimConsumerManagerPort;
import com.github.swim_developer.framework.application.port.out.SwimSubscriptionFilterPort;
import com.github.swim_developer.framework.consumer.application.subscription.service.AbstractSubscriptionService;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import com.github.swim_developer.framework.domain.exception.SubscriptionNotFoundException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

import static com.github.swim_developer.framework.domain.model.SubscriptionType.DECLARED;
import static com.github.swim_developer.framework.domain.model.SubscriptionType.ON_DEMAND;

@Slf4j
@ApplicationScoped
public class Ed254SubscriptionUseCase extends AbstractSubscriptionService<DesiredSubscription, Subscription>
        implements ManageSubscriptionPort {

    private static final String SUBSCRIPTION_NOT_FOUND = "Subscription not found: ";

    private final SubscriptionStore repository;
    private final RemoteSubscriptionManagerPort smPort;
    private final SwimProviderConfigPort providerConfigParser;
    private final SwimSubscriptionFilterPort filterCache;
    private final Ed254SequenceGapCachePort sequenceGapCache;

    Ed254SubscriptionUseCase() {
        this.repository = null;
        this.smPort = null;
        this.providerConfigParser = null;
        this.filterCache = null;
        this.sequenceGapCache = null;
    }

    @Inject
    public Ed254SubscriptionUseCase(SubscriptionStore repository,
                                    RemoteSubscriptionManagerPort smPort,
                                    SwimProviderConfigPort providerConfigParser,
                                    SwimConsumerManagerPort consumerManager,
                                    SwimSubscriptionFilterPort filterCache,
                                    Ed254SequenceGapCachePort sequenceGapCache) {
        super(consumerManager);
        this.repository = repository;
        this.smPort = smPort;
        this.providerConfigParser = providerConfigParser;
        this.filterCache = filterCache;
        this.sequenceGapCache = sequenceGapCache;
    }

    @Override
    protected List<Subscription> findActiveSubscriptions() {
        return repository.findActiveSubscriptions();
    }

    @Override
    protected void onAllConsumersRegistered() {
        populateFilterCache();
        sequenceGapCache.loadAll();
    }

    public void populateFilterCache() {
        repository.findActiveSubscriptions().forEach(this::cacheFilters);
        log.info("Subscription filter cache populated with {} entries", filterCache.size());
    }

    @Override
    public Subscription createSubscription(DesiredSubscription desired) {
        String configHash = desired.generateConfigHash();

        Optional<Subscription> existing = repository.findByConfigHash(configHash);
        if (existing.isPresent()) {
            log.warn("Subscription with same configuration already exists: {}", existing.get().getSubscriptionId());
            return existing.get();
        }

        ProviderConfiguration provider = resolveProvider(desired.provider());
        Subscription subscription = smPort.createSubscription(desired, provider);
        subscription.setType(ON_DEMAND.name());
        subscription.setConfigHash(configHash);

        repository.persistSubscription(subscription);
        cacheFilters(subscription);

        activateSubscription(subscription.getSubscriptionId(), subscription.getQueueName(), provider);
        return repository.findBySubscriptionId(subscription.getSubscriptionId()).orElse(subscription);
    }

    @Override
    protected Subscription callCreateAndPersist(DesiredSubscription desired) {
        String configHash = desired.generateConfigHash();
        ProviderConfiguration provider = resolveProvider(desired.provider());
        Subscription remote = smPort.createSubscription(desired, provider);

        Optional<Subscription> duplicate = repository.findBySubscriptionId(remote.getSubscriptionId());
        if (duplicate.isPresent()) {
            Subscription existing = duplicate.get();
            existing.setQueueName(remote.getQueueName());
            existing.setSubscriptionStatus(remote.getSubscriptionStatus());
            existing.setDestinationAerodrome(desired.destinationAerodrome());
            existing.setPointName(desired.pointName());
            existing.setFlightSelector(desired.flightSelector());
            existing.setAnySupplementaryData(desired.supplementaryData() != null && desired.supplementaryData().anyRequested());
            existing.setDescription(desired.description());
            existing.setConfigHash(configHash);
            existing.setProviderId(desired.provider());
            repository.updateSubscription(existing);
            cacheFilters(existing);
            return existing;
        }

        remote.setType(DECLARED.name());
        remote.setConfigHash(configHash);
        repository.persistSubscription(remote);
        cacheFilters(remote);
        return remote;
    }

    @Override
    protected String callUpdateStatus(String subscriptionId, String newStatus) {
        try {
            ProviderConfiguration provider = resolveProviderForSubscription(subscriptionId);
            return smPort.updateSubscriptionStatus(subscriptionId, newStatus, provider);
        } catch (WebApplicationException e) {
            int status = e.getResponse().getStatus();
            if (status == 404 || status == 410) {
                throw new SubscriptionNotFoundException(subscriptionId, e);
            }
            throw e;
        }
    }

    @Override
    protected void callDeleteRemoteSubscription(String subscriptionId) {
        ProviderConfiguration provider = resolveProviderForSubscription(subscriptionId);
        smPort.deleteSubscription(subscriptionId, provider);
    }

    @Override
    protected boolean existsLocally(DesiredSubscription desired) {
        String configHash = desired.generateConfigHash();
        return repository.findByConfigHashAndType(configHash, DECLARED.name()).isPresent();
    }

    @Override
    public Optional<Subscription> findBySubscriptionId(String subscriptionId) {
        return repository.findBySubscriptionId(subscriptionId);
    }

    @Override
    protected List<Subscription> loadDeclaredSubscriptions() {
        return repository.findDeclaredSubscriptions();
    }

    @Override
    protected boolean isStillDesired(Subscription current, List<DesiredSubscription> desiredSubscriptions) {
        return desiredSubscriptions.stream()
                .anyMatch(desired -> desired.generateConfigHash().equals(current.getConfigHash()));
    }

    @Override
    protected void deleteLocalSubscription(String subscriptionId) {
        repository.findBySubscriptionId(subscriptionId)
                .ifPresent(sub -> sequenceGapCache.removeSubscription(sub.getQueueName()));
        filterCache.removeSubscription(subscriptionId);
        repository.deleteBySubscriptionId(subscriptionId);
    }

    @Override
    protected void updateLocalStatus(String subscriptionId, String status) {
        repository.updateStatus(subscriptionId, status);
    }

    @Override
    protected String describeDesired(DesiredSubscription desired) {
        return desired.description();
    }

    @Override
    protected Optional<DesiredSubscription> toDesiredConfig(Subscription subscription) {
        return Optional.of(new DesiredSubscription(
                subscription.getProviderId(),
                subscription.getDestinationAerodrome(),
                subscription.getPointName(),
                subscription.getFlightSelector(),
                null,
                subscription.getDescription()
        ));
    }

    @Override
    public ProviderConfiguration resolveProvider(String providerId) {
        return providerConfigParser.findByProviderIdOrDefault(providerId)
                .orElseThrow(() -> new IllegalStateException("Provider not configured: " + providerId));
    }

    @Override
    public void deleteSubscriptionById(String subscriptionId) {
        callDeleteRemoteSubscription(subscriptionId);
        deleteLocalSubscription(subscriptionId);
    }

    private ProviderConfiguration resolveProviderForSubscription(String subscriptionId) {
        Subscription sub = repository.findBySubscriptionId(subscriptionId)
                .orElseThrow(() -> new IllegalArgumentException(SUBSCRIPTION_NOT_FOUND + subscriptionId));
        return resolveProvider(sub.getProviderId());
    }

    private void cacheFilters(Subscription sub) {
        String id = sub.getSubscriptionId();
        filterCache.updateFilters(id, FilterDimension.AERODROME, sub.extractAerodromeDesignators());
        filterCache.updateFilters(id, FilterDimension.SUPPLEMENTARY_DATA,
                List.of(String.valueOf(sub.isAnySupplementaryData())));
    }
}
