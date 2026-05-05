package com.github.swim_developer.ed254.consumer.domain.model;

import java.util.ArrayList;
import java.util.List;

public class ParsedSequence {
    private boolean firstMessageAfterServiceOutage;
    public final List<String> allFlights = new ArrayList<>();
    public final List<String> continuingFlights = new ArrayList<>();

    public boolean isFirstMessageAfterServiceOutage() {
        return firstMessageAfterServiceOutage;
    }

    public void setFirstMessageAfterServiceOutage(boolean firstMessageAfterServiceOutage) {
        this.firstMessageAfterServiceOutage = firstMessageAfterServiceOutage;
    }
}
