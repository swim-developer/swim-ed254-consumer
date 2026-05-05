package com.github.swim_developer.ed254.consumer.domain.model;


import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record SupplementaryData(
        boolean delay,
        boolean landingSequencePosition,
        boolean amanStrategy,
        boolean departureAerodrome,
        boolean proposedProcedure
) {
    public boolean anyRequested() {
        return delay || landingSequencePosition || amanStrategy || departureAerodrome || proposedProcedure;
    }
}
