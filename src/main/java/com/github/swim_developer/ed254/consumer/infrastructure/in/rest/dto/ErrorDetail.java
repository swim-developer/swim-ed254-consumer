package com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record ErrorDetail(
        String erroneousFieldName,
        String errorCode,
        String errorMessage
) {}
