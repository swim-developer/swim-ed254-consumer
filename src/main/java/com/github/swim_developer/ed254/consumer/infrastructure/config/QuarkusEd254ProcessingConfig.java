package com.github.swim_developer.ed254.consumer.infrastructure.config;

import com.github.swim_developer.ed254.consumer.application.port.in.Ed254ProcessingConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class QuarkusEd254ProcessingConfig implements Ed254ProcessingConfig {

    private final long messageValidityThresholdMs;

    @Inject
    public QuarkusEd254ProcessingConfig(
            @ConfigProperty(name = "ed254.validation.message-validity-threshold-ms", defaultValue = "30000")
            long messageValidityThresholdMs) {
        this.messageValidityThresholdMs = messageValidityThresholdMs;
    }

    @Override
    public long messageValidityThresholdMs() {
        return messageValidityThresholdMs;
    }
}
