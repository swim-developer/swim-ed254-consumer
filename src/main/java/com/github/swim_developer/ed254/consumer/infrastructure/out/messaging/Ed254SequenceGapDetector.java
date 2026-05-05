package com.github.swim_developer.ed254.consumer.infrastructure.out.messaging;

import aero.fixm.ed254.ArrivalSequence;
import com.github.swim_developer.ed254.consumer.application.port.out.Ed254SequenceGapPort;
import com.github.swim_developer.ed254.consumer.domain.model.ParsedSequence;
import com.github.swim_developer.ed254.consumer.infrastructure.out.idempotency.Ed254SequenceGapCache;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
@ApplicationScoped
public class Ed254SequenceGapDetector implements Ed254SequenceGapPort {

    private final Ed254SequenceGapCache gapCache;
    private final MeterRegistry meterRegistry;

    @Inject
    public Ed254SequenceGapDetector(Ed254SequenceGapCache gapCache, MeterRegistry meterRegistry) {
        this.gapCache = gapCache;
        this.meterRegistry = meterRegistry;
    }

    public Optional<DataValidationResult> detect(String queueName, ArrivalSequence sequence) {
        try {
            ParsedSequence parsed = extractFlights(sequence);

            if (parsed.isFirstMessageAfterServiceOutage()) {
                log.info("firstMessageAfterServiceOutage=true on queue {} - resetting state", queueName);
                gapCache.updateArcids(queueName, parsed.continuingFlights);
                return Optional.empty();
            }

            List<String> previousArcids = gapCache.getArcids(queueName);

            if (previousArcids == null) {
                gapCache.updateArcids(queueName, parsed.continuingFlights);
                return Optional.empty();
            }

            List<String> missingFlights = previousArcids.stream()
                    .filter(flight -> !parsed.allFlights.contains(flight))
                    .toList();

            gapCache.updateArcids(queueName, parsed.continuingFlights);

            if (!missingFlights.isEmpty()) {
                log.debug("Sequence gap detected on queue {} - missing flights: {}", queueName, missingFlights);
                meterRegistry.counter("ed254_sequence_gaps", "queue", queueName)
                        .increment(missingFlights.size());
                return Optional.of(DataValidationResult.sequenceGaps(missingFlights));
            }

            return Optional.empty();
        } catch (Exception e) {
            log.error("Failed to detect sequence gaps on queue {}: {}", queueName, e.getMessage());
            return Optional.empty();
        }
    }

    private ParsedSequence extractFlights(ArrivalSequence sequence) {
        ParsedSequence result = new ParsedSequence();
        result.setFirstMessageAfterServiceOutage(sequence.isFirstMessageAfterServiceOutage());

        if (sequence.getSequenceEntries() == null
                || sequence.getSequenceEntries().getArrivalManagementInformation() == null) {
            return result;
        }

        for (var entry : sequence.getSequenceEntries().getArrivalManagementInformation()) {
            if (entry.getFlightIdentification() == null) {
                continue;
            }
            String arcid = entry.getFlightIdentification().getArcid();
            if (arcid != null) {
                result.allFlights.add(arcid);
                if (!entry.isLastFiledRecord()) {
                    result.continuingFlights.add(arcid);
                }
            }
        }

        return result;
    }
}
