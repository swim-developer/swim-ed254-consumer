package com.github.swim_developer.ed254.consumer.application.service;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.FilterDimension;
import com.github.swim_developer.framework.application.port.out.SwimDeadLetterPort;
import com.github.swim_developer.framework.application.port.out.SwimSubscriptionFilterPort;
import com.github.swim_developer.framework.consumer.application.messaging.processing.AbstractEventFilterService;
import com.github.swim_developer.framework.consumer.application.messaging.processing.FilterRule;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class Ed254EventFilterService extends AbstractEventFilterService<ArrivalEvent> {

    @Inject
    public Ed254EventFilterService(SwimSubscriptionFilterPort filterCache,
                                   SwimDeadLetterPort deadLetterService) {
        super(filterCache, deadLetterService);
    }

    @Override
    protected List<FilterRule<ArrivalEvent>> buildFilterRules(ArrivalEvent event) {
        return List.of(
                new FilterRule<>(FilterDimension.AERODROME, ArrivalEvent::getAerodromeDesignator)
        );
    }
}
