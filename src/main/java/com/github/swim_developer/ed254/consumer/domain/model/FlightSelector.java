package com.github.swim_developer.ed254.consumer.domain.model;


import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlightSelector(
        String arcid,
        String ades,
        String adep,
        String eobt,
        String eobd,
        String ifplId
) {}
