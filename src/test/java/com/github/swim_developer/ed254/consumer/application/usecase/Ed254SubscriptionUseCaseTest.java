package com.github.swim_developer.ed254.consumer.application.usecase;

import com.github.swim_developer.ed254.consumer.application.port.out.Ed254SequenceGapCachePort;
import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.ed254.consumer.domain.model.DestinationAerodrome;
import com.github.swim_developer.ed254.consumer.domain.model.FilterDimension;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.domain.port.out.RemoteSubscriptionManagerPort;
import com.github.swim_developer.ed254.consumer.domain.port.out.SubscriptionStore;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import com.github.swim_developer.framework.application.port.out.SwimConsumerManagerPort;
import com.github.swim_developer.framework.application.port.out.SwimProviderConfigPort;
import com.github.swim_developer.framework.application.port.out.SwimSubscriptionFilterPort;
import com.github.swim_developer.framework.domain.model.SubscriptionStatus;
import com.github.swim_developer.framework.infrastructure.testing.TestNameLoggerExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(TestNameLoggerExtension.class)
class Ed254SubscriptionUseCaseTest {

    private SubscriptionStore repository;
    private RemoteSubscriptionManagerPort smPort;
    private SwimProviderConfigPort providerConfigParser;
    private SwimConsumerManagerPort consumerManager;
    private SwimSubscriptionFilterPort filterCache;
    private Ed254SequenceGapCachePort sequenceGapCache;
    private Ed254SubscriptionUseCase useCase;

    private static final ProviderConfiguration PROVIDER = ProviderConfiguration.builder()
            .providerId("default")
            .build();

    @BeforeEach
    void setUp() {
        repository = mock(SubscriptionStore.class);
        smPort = mock(RemoteSubscriptionManagerPort.class);
        providerConfigParser = mock(SwimProviderConfigPort.class);
        consumerManager = mock(SwimConsumerManagerPort.class);
        filterCache = mock(SwimSubscriptionFilterPort.class);
        sequenceGapCache = mock(Ed254SequenceGapCachePort.class);
        useCase = new Ed254SubscriptionUseCase(repository, smPort, providerConfigParser,
                consumerManager, filterCache, sequenceGapCache);
    }

    @Test
    void populateFilterCache_updatesFiltersForAllActiveSubscriptions() {
        Subscription sub = subscriptionWith("sub-1", "queue-1");
        sub.setDestinationAerodrome(List.of(new DestinationAerodrome("LPPT", null)));
        sub.setAnySupplementaryData(true);
        when(repository.findActiveSubscriptions()).thenReturn(List.of(sub));

        useCase.populateFilterCache();

        verify(filterCache).updateFilters("sub-1", FilterDimension.AERODROME, List.of("LPPT"));
        verify(filterCache).updateFilters("sub-1", FilterDimension.SUPPLEMENTARY_DATA, List.of("true"));
    }

    @Test
    void createSubscription_returnsExisting_whenConfigHashMatches() {
        DesiredSubscription desired = desiredSubscriptionWith("default");
        String configHash = desired.generateConfigHash();
        Subscription existing = subscriptionWith("sub-existing", "queue-existing");
        when(repository.findByConfigHash(configHash)).thenReturn(Optional.of(existing));

        Subscription result = useCase.createSubscription(desired);

        assertThat(result).isSameAs(existing);
        verify(smPort, never()).createSubscription(any(), any());
    }

    @Test
    void createSubscription_createsNew_whenNoExistingHash() {
        DesiredSubscription desired = desiredSubscriptionWith("default");
        String configHash = desired.generateConfigHash();

        Subscription remote = subscriptionWith("sub-new", "ED254.v1.user.sub-new");
        remote.setSubscriptionStatus(SubscriptionStatus.PAUSED.name());
        remote.setProviderId("default");

        when(repository.findByConfigHash(configHash)).thenReturn(Optional.empty());
        when(providerConfigParser.findByProviderIdOrDefault("default")).thenReturn(Optional.of(PROVIDER));
        when(smPort.createSubscription(desired, PROVIDER)).thenReturn(remote);
        when(smPort.updateSubscriptionStatus("sub-new", "ACTIVE", PROVIDER)).thenReturn("ACTIVE");
        when(repository.findBySubscriptionId("sub-new")).thenReturn(Optional.of(remote));

        Subscription result = useCase.createSubscription(desired);

        assertThat(result).isNotNull();
        verify(repository).persistSubscription(remote);
    }

    @Test
    void findBySubscriptionId_delegatesToRepository() {
        Subscription sub = subscriptionWith("sub-1", "queue-1");
        when(repository.findBySubscriptionId("sub-1")).thenReturn(Optional.of(sub));

        Optional<Subscription> result = useCase.findBySubscriptionId("sub-1");

        assertThat(result).contains(sub);
    }

    @Test
    void findBySubscriptionId_returnsEmpty_whenNotFound() {
        when(repository.findBySubscriptionId("sub-unknown")).thenReturn(Optional.empty());

        Optional<Subscription> result = useCase.findBySubscriptionId("sub-unknown");

        assertThat(result).isEmpty();
    }

    @Test
    void resolveProvider_returnsProvider_whenConfigured() {
        when(providerConfigParser.findByProviderIdOrDefault("default")).thenReturn(Optional.of(PROVIDER));

        ProviderConfiguration result = useCase.resolveProvider("default");

        assertThat(result).isEqualTo(PROVIDER);
    }

    @Test
    void resolveProvider_throwsIllegalState_whenProviderNotFound() {
        when(providerConfigParser.findByProviderIdOrDefault("unknown")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> useCase.resolveProvider("unknown"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown");
    }

    @Test
    void toDesiredConfig_mapsSubscriptionToDesiredSubscription() {
        Subscription sub = subscriptionWith("sub-1", "queue-1");
        sub.setDestinationAerodrome(List.of(new DestinationAerodrome("LPPT", null)));
        sub.setDescription("ED-254 test sub");
        sub.setProviderId("provider-y");

        Optional<DesiredSubscription> result = useCase.toDesiredConfig(sub);

        assertThat(result).isPresent();
        assertThat(result.get().provider()).isEqualTo("provider-y");
        assertThat(result.get().description()).isEqualTo("ED-254 test sub");
        assertThat(result.get().extractAerodromeDesignators()).containsExactly("LPPT");
    }

    @Test
    void describeDesired_returnsDesiredDescription() {
        DesiredSubscription desired = desiredSubscriptionWith("default");

        String description = useCase.describeDesired(desired);

        assertThat(description).isEqualTo("ED-254 test sub");
    }

    @Test
    void isStillDesired_returnsTrue_whenHashMatches() {
        DesiredSubscription desired = desiredSubscriptionWith("default");
        Subscription current = subscriptionWith("sub-1", "queue-1");
        current.setConfigHash(desired.generateConfigHash());

        boolean result = useCase.isStillDesired(current, List.of(desired));

        assertThat(result).isTrue();
    }

    @Test
    void isStillDesired_returnsFalse_whenHashNotInList() {
        DesiredSubscription desired = desiredSubscriptionWith("default");
        Subscription current = subscriptionWith("sub-1", "queue-1");
        current.setConfigHash("completely-different-hash");

        boolean result = useCase.isStillDesired(current, List.of(desired));

        assertThat(result).isFalse();
    }

    private Subscription subscriptionWith(String subscriptionId, String queueName) {
        Subscription sub = new Subscription();
        sub.setSubscriptionId(subscriptionId);
        sub.setQueueName(queueName);
        return sub;
    }

    private DesiredSubscription desiredSubscriptionWith(String providerId) {
        return new DesiredSubscription(
                providerId,
                List.of(new DestinationAerodrome("LPPT", null)),
                List.of("AMSTR"),
                null,
                null,
                "ED-254 test sub"
        );
    }
}
