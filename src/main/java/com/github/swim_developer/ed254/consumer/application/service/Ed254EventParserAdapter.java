package com.github.swim_developer.ed254.consumer.application.service;

import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import com.github.swim_developer.framework.application.port.out.SwimXmlUnmarshallerPort;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventParser;
import com.github.swim_developer.framework.domain.exception.XmlValidationException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class Ed254EventParserAdapter implements SwimEventParser<Ed254Message> {

    private final SwimXmlUnmarshallerPort<Ed254Message> jaxbPool;
    private final Ed254ParsedMessageHolder parsedMessageHolder;

    @Inject
    public Ed254EventParserAdapter(SwimXmlUnmarshallerPort<Ed254Message> jaxbPool,
                                  Ed254ParsedMessageHolder parsedMessageHolder) {
        this.jaxbPool = jaxbPool;
        this.parsedMessageHolder = parsedMessageHolder;
    }

    @Override
    public Ed254Message unmarshalAndValidate(String xmlPayload) throws XmlValidationException {
        Ed254Message parsed = jaxbPool.unmarshalAndValidate(xmlPayload);
        parsedMessageHolder.set(parsed);
        return parsed;
    }
}
