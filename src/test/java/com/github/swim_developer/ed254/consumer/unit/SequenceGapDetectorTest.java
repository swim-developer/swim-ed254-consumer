package com.github.swim_developer.ed254.consumer.unit;

import aero.fixm.ed254.ArrivalSequence;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import com.github.swim_developer.framework.domain.model.ValidationResultType;
import com.github.swim_developer.ed254.consumer.infrastructure.out.idempotency.Ed254SequenceGapCache;
import com.github.swim_developer.ed254.consumer.infrastructure.out.messaging.Ed254SequenceGapDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SequenceGapDetectorTest {

    private Ed254SequenceGapDetector detector;
    private Ed254SequenceGapCache gapCache;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setup(TestInfo testInfo) {
        System.out.printf("%n══ ▶ %s.%s%n", getClass().getSimpleName(), testInfo.getDisplayName());
        gapCache = mock(Ed254SequenceGapCache.class);
        meterRegistry = new SimpleMeterRegistry();
        detector = new Ed254SequenceGapDetector(gapCache, meterRegistry);
    }

    @Test
    void firstMessage_noPreviousState_noGapReported() {
        when(gapCache.getArcids("q-1")).thenReturn(null);

        Optional<DataValidationResult> result = detector.detect("q-1", buildSequence(false,
                flight("TAP1234", false),
                flight("RYR5678", false)));

        assertThat(result).isEmpty();
        verify(gapCache).updateArcids("q-1", List.of("TAP1234", "RYR5678"));
    }

    @Test
    void consecutiveMessages_noGap() {
        when(gapCache.getArcids("q-1")).thenReturn(List.of("TAP1234", "RYR5678"));

        Optional<DataValidationResult> result = detector.detect("q-1", buildSequence(false,
                flight("TAP1234", false),
                flight("RYR5678", false),
                flight("BAW9999", false)));

        assertThat(result).isEmpty();
    }

    @Test
    void flightDisappears_gapDetected() {
        when(gapCache.getArcids("q-1")).thenReturn(List.of("TAP1234", "RYR5678"));

        Optional<DataValidationResult> result = detector.detect("q-1", buildSequence(false,
                flight("RYR5678", false)));

        assertThat(result).isPresent();
        assertThat(result.get().dataValResult()).isEqualTo(ValidationResultType.SEQUENCE_GAPS);
        assertThat(result.get().errorReport()).hasSize(1);
        assertThat(result.get().errorReport().get(0).errorMessage()).contains("TAP1234");
    }

    @Test
    void flightWithLastFiledRecord_noGapWhenAllPresent() {
        when(gapCache.getArcids("q-1")).thenReturn(List.of("TAP1234", "RYR5678"));

        Optional<DataValidationResult> result = detector.detect("q-1", buildSequence(false,
                flight("TAP1234", true),
                flight("RYR5678", false)));

        assertThat(result).isEmpty();
    }

    @Test
    void firstMessageAfterServiceOutage_resetsState_noGap() {
        when(gapCache.getArcids("q-1")).thenReturn(List.of("TAP1234", "RYR5678"));

        Optional<DataValidationResult> result = detector.detect("q-1", buildSequence(true,
                flight("BAW9999", false)));

        assertThat(result).isEmpty();
        verify(gapCache).updateArcids("q-1", List.of("BAW9999"));
    }

    @Test
    void differentQueues_independentStates() {
        when(gapCache.getArcids("q-1")).thenReturn(List.of("TAP1234"));
        when(gapCache.getArcids("q-2")).thenReturn(null);

        Optional<DataValidationResult> r1 = detector.detect("q-1", buildSequence(false,
                flight("BAW0001", false)));
        Optional<DataValidationResult> r2 = detector.detect("q-2", buildSequence(false,
                flight("TAP1234", false)));

        assertThat(r1).isPresent();
        assertThat(r2).isEmpty();
    }

    @Test
    void gapDetected_incrementsMetric() {
        when(gapCache.getArcids("q-1")).thenReturn(List.of("TAP1234", "RYR5678"));

        detector.detect("q-1", buildSequence(false, flight("RYR5678", false)));

        assertThat(meterRegistry.find("ed254_sequence_gaps")
                .tag("queue", "q-1")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void lastFiledFlight_excludedFromContinuingList() {
        when(gapCache.getArcids("q-1")).thenReturn(null);

        detector.detect("q-1", buildSequence(false,
                flight("TAP1234", true),
                flight("RYR5678", false)));

        verify(gapCache).updateArcids("q-1", List.of("RYR5678"));
    }

    private ArrivalSequence buildSequence(boolean serviceOutage,
                                          ArrivalSequence.SequenceEntries.ArrivalManagementInformation... flights) {
        ArrivalSequence seq = new ArrivalSequence();
        seq.setFirstMessageAfterServiceOutage(serviceOutage);
        seq.setAerodromeDesignator("ESSA");

        ArrivalSequence.SequenceEntries entries = new ArrivalSequence.SequenceEntries();
        for (var f : flights) {
            entries.getArrivalManagementInformation().add(f);
        }
        seq.setSequenceEntries(entries);
        return seq;
    }

    private ArrivalSequence.SequenceEntries.ArrivalManagementInformation flight(String arcid, boolean lastFiled) {
        var flightId = new ArrivalSequence.SequenceEntries.ArrivalManagementInformation.FlightIdentification();
        flightId.setArcid(arcid);

        var ami = new ArrivalSequence.SequenceEntries.ArrivalManagementInformation();
        ami.setLastFiledRecord(lastFiled);
        ami.setFlightIdentification(flightId);
        return ami;
    }
}
