package com.github.swim_developer.ed254.consumer.application.service;

import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Ed254ParsedMessageHolder {

    private static final ThreadLocal<Ed254Message> PARSED = new ThreadLocal<>();

    public void set(Ed254Message message) {
        PARSED.set(message);
    }

    public Ed254Message get() {
        return PARSED.get();
    }

    public void remove() {
        PARSED.remove();
    }
}
