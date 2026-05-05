package com.github.swim_developer.ed254.consumer.application.port.out;

import java.util.List;

public interface Ed254SequenceGapCachePort {

    List<String> getArcids(String queueName);

    void updateArcids(String queueName, List<String> arcids);

    void removeSubscription(String queueName);

    void loadAll();
}
