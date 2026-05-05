package com.github.swim_developer.ed254.consumer.domain.model;

import aero.fixm.ed254.ArrivalSequence;
import aero.fixm.ed254.ProviderExceptions;

public sealed interface Ed254Message
        permits Ed254Message.ArrivalMsg, Ed254Message.ExceptionMsg {

    record ArrivalMsg(ArrivalSequence payload) implements Ed254Message {}

    record ExceptionMsg(ProviderExceptions payload) implements Ed254Message {}
}
