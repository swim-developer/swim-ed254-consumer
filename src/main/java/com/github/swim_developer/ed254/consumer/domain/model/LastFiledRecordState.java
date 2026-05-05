package com.github.swim_developer.ed254.consumer.domain.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LastFiledRecordState {

    private String id;
    private String queueName;
    private List<String> arcids;
    private Instant updatedAt;
}
