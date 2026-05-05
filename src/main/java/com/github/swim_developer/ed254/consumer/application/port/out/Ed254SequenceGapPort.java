package com.github.swim_developer.ed254.consumer.application.port.out;

import aero.fixm.ed254.ArrivalSequence;
import com.github.swim_developer.framework.domain.model.DataValidationResult;

import java.util.Optional;

public interface Ed254SequenceGapPort {

    Optional<DataValidationResult> detect(String queueName, ArrivalSequence sequence);
}
