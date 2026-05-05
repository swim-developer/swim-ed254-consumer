package com.github.swim_developer.ed254.consumer.infrastructure.out.xml;

import aero.fixm.ed254.ArrivalSequence;
import aero.fixm.ed254.ProviderExceptions;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import com.github.swim_developer.ed254.consumer.domain.model.Ed254MessageType;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.application.port.out.SwimEventExtractor;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

import javax.xml.datatype.XMLGregorianCalendar;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Slf4j
@ApplicationScoped
public class Ed254EventExtractor implements SwimEventExtractor<ArrivalEvent, Ed254Message> {

    @Override
    public String getTypeLabel(ArrivalEvent event) {
        return event.getMessageType() != null ? event.getMessageType() : "unknown";
    }

    @Override
    public List<Optional<ArrivalEvent>> extract(Ed254Message message) {
        if (message == null) {
            return List.of(Optional.empty());
        }

        return switch (message) {
            case Ed254Message.ArrivalMsg(ArrivalSequence seq) -> extractFromArrivalSequence(seq);
            case Ed254Message.ExceptionMsg(ProviderExceptions ignored) -> extractFromProviderExceptions();
        };
    }

    private List<Optional<ArrivalEvent>> extractFromArrivalSequence(ArrivalSequence seq) {
        if (seq.getAerodromeDesignator() == null) {
            return List.of(Optional.empty());
        }

        ArrivalEvent event = new ArrivalEvent();
        event.setReceivedAt(Instant.now());
        event.setAerodromeDesignator(seq.getAerodromeDesignator());
        event.setPublicationTime(toInstant(seq.getPublicationTime()));
        event.setCreationTime(toInstant(seq.getCreationTime()));
        event.setFirstMessageAfterServiceOutage(seq.isFirstMessageAfterServiceOutage());
        event.setMessageType(Ed254MessageType.ARRIVAL_SEQUENCE);
        event.setDeliveryStatus(OutboxDeliveryStatus.PENDING);

        if (seq.getSequenceEntries() != null && seq.getSequenceEntries().getArrivalManagementInformation() != null) {
            event.setSequenceEntriesCount(seq.getSequenceEntries().getArrivalManagementInformation().size());
        }

        return List.of(Optional.of(event));
    }

    private List<Optional<ArrivalEvent>> extractFromProviderExceptions() {
        ArrivalEvent event = new ArrivalEvent();
        event.setReceivedAt(Instant.now());
        event.setMessageType(Ed254MessageType.PROVIDER_EXCEPTION);
        event.setDeliveryStatus(OutboxDeliveryStatus.PENDING);
        return List.of(Optional.of(event));
    }

    private static Instant toInstant(XMLGregorianCalendar cal) {
        if (cal == null) {
            return null;
        }
        return cal.toGregorianCalendar().toInstant();
    }
}
