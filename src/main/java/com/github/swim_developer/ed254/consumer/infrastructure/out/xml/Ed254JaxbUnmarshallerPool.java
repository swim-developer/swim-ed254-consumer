package com.github.swim_developer.ed254.consumer.infrastructure.out.xml;

import aero.fixm.ed254.ArrivalSequence;
import aero.fixm.ed254.ProviderExceptions;
import aero.fixm.validation.Ed254UnmarshallerPool;
import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import com.github.swim_developer.framework.application.port.out.SwimXmlUnmarshallerPort;
import com.github.swim_developer.framework.domain.exception.XmlValidationException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class Ed254JaxbUnmarshallerPool implements SwimXmlUnmarshallerPort<Ed254Message> {

    private final Ed254UnmarshallerPool pool;

    @Inject
    public Ed254JaxbUnmarshallerPool() {
        this.pool = new Ed254UnmarshallerPool();
    }

    @PostConstruct
    void logInitialization() {
        log.info("ED-254 JAXB unmarshaller pool initialized from fixm-ed254-model");
    }

    @Override
    public Ed254Message unmarshalAndValidate(String xml) throws XmlValidationException {
        try {
            Object result = pool.unmarshalAndValidate(xml);
            return convert(result);
        } catch (Ed254UnmarshallerPool.Ed254UnmarshalException e) {
            throw new XmlValidationException(e.getMessage(), e);
        }
    }

    private Ed254Message convert(Object unmarshalled) {
        if (unmarshalled instanceof ArrivalSequence as) {
            return new Ed254Message.ArrivalMsg(as);
        }
        if (unmarshalled instanceof ProviderExceptions pe) {
            return new Ed254Message.ExceptionMsg(pe);
        }
        throw new IllegalStateException("Unexpected JAXB type: " + unmarshalled.getClass().getName());
    }
}
