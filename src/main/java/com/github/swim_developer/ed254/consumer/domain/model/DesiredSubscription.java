package com.github.swim_developer.ed254.consumer.domain.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

@RegisterForReflection
public record DesiredSubscription(
        String provider,
        List<DestinationAerodrome> destinationAerodrome,
        List<String> pointName,
        List<FlightSelector> flightSelector,
        SupplementaryData supplementaryData,
        String description
) {

    public String generateConfigHash() {
        String prov = provider != null ? provider : "";
        String aerodromes = destinationAerodrome != null
                ? destinationAerodrome.stream()
                    .map(DestinationAerodrome::aerodromeDesignator)
                    .sorted()
                    .collect(Collectors.joining(","))
                : "";
        String points = pointName != null
                ? pointName.stream().sorted().collect(Collectors.joining(","))
                : "";
        String suppData = supplementaryData != null
                ? String.valueOf(supplementaryData.anyRequested())
                : "false";
        return sha256(prov + aerodromes + points + suppData);
    }

    private static String sha256(String content) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    public List<String> extractAerodromeDesignators() {
        if (destinationAerodrome == null || destinationAerodrome.isEmpty()) {
            return Collections.emptyList();
        }
        return destinationAerodrome.stream()
                .map(DestinationAerodrome::aerodromeDesignator)
                .toList();
    }
}
