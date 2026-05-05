package com.github.swim_developer.ed254.consumer.infrastructure.in.rest;

import com.github.swim_developer.ed254.consumer.infrastructure.out.mapper.Ed254ConsumerMapper;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.EventDTO;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoArrivalEventStore;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.ArrivalEventDocument;
import com.github.swim_developer.framework.infrastructure.in.rest.PageResponse;
import static com.github.swim_developer.framework.consumer.infrastructure.in.rest.ConsumerRestResponses.*;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "SWIM ED-254 Consumer Events", description = "Arrival event query and retrieval")
public class ConsumerEventResource {

    private final MongoArrivalEventStore eventRepository;
    private final Ed254ConsumerMapper mapper;

    @Inject
    public ConsumerEventResource(MongoArrivalEventStore eventRepository, Ed254ConsumerMapper mapper) {
        this.eventRepository = eventRepository;
        this.mapper = mapper;
    }

    @GET
    @Path("/events")
    @Operation(summary = "List events", description = "Retrieves arrival events with optional aerodrome filter and pagination")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "Paginated list of events",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = PageResponse.class)))
    })
    public Response listEvents(
            @Parameter(description = "Aerodrome ICAO filter", example = "LPPT")
            @QueryParam("aerodrome") String aerodrome,
            @Parameter(description = "Page number (0-indexed)", example = "0")
            @QueryParam("page") @DefaultValue("0") int page,
            @Parameter(description = "Page size", example = "20")
            @QueryParam("size") @DefaultValue("20") int size) {

        List<ArrivalEvent> events;
        long totalElements;

        if (aerodrome != null && !aerodrome.isBlank()) {
            events = eventRepository.<ArrivalEventDocument>find("aerodromeDesignator", aerodrome)
                    .page(page, size).list().stream().map(ArrivalEventDocument::toDomain).toList();
            totalElements = eventRepository.countByAerodrome(aerodrome);
        } else {
            events = eventRepository.<ArrivalEventDocument>findAll().page(page, size).list()
                    .stream().map(ArrivalEventDocument::toDomain).toList();
            totalElements = eventRepository.count();
        }

        List<EventDTO> dtos = events.stream()
                .map(mapper::toDTO)
                .toList();

        return Response.ok(PageResponse.of(dtos, page, size, totalElements)).build();
    }

    @GET
    @Path("/events/range")
    @Operation(summary = "List events by date range", description = "Retrieves arrival events with receivedAt between startDate and endDate (optional aerodrome filter)")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "Paginated list of events",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = PageResponse.class))),
            @APIResponse(responseCode = "400", description = "Invalid parameters")
    })
    public Response listEventsByDateRange(
            @Parameter(description = "Start (ISO-8601)", required = true)
            @QueryParam("startDate") String startDateStr,
            @Parameter(description = "End (ISO-8601)", required = true)
            @QueryParam("endDate") String endDateStr,
            @Parameter(description = "Aerodrome ICAO filter")
            @QueryParam("aerodrome") String aerodrome,
            @Parameter(description = "Page number (0-indexed)")
            @QueryParam("page") @DefaultValue("0") int page,
            @Parameter(description = "Page size")
            @QueryParam("size") @DefaultValue("20") int size) {

        if (startDateStr == null || endDateStr == null) {
            return badRequest("startDate and endDate are required parameters");
        }
        final Instant startDate;
        final Instant endDate;
        try {
            startDate = parseDateOrThrow(startDateStr);
            endDate = parseDateOrThrow(endDateStr);
            if (!isValidDateRange(startDate, endDate)) {
                return badRequest("startDate must be before or equal to endDate");
            }
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        List<ArrivalEvent> events = eventRepository.findByReceivedAtBetween(startDate, endDate, aerodrome, page, size);
        long totalElements = eventRepository.countByReceivedAtBetween(startDate, endDate, aerodrome);
        List<EventDTO> dtos = events.stream().map(mapper::toDTO).toList();
        return Response.ok(PageResponse.of(dtos, page, size, totalElements)).build();
    }

    @GET
    @Path("/events/{messageId}")
    @Operation(summary = "Get event by ID", description = "Retrieves a specific arrival event by its message ID")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "Event found",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = EventDTO.class))),
            @APIResponse(responseCode = "404", description = "Event not found")
    })
    public Response getEvent(
            @Parameter(description = "Message ID", required = true, example = "ID:abc-123-def#0")
            @PathParam("messageId") String messageId) {

        return eventRepository.findByMessageId(messageId)
                .map(e -> ok(mapper.toDTO(e)))
                .orElse(notFound("Event not found: " + messageId));
    }

    @GET
    @Path("/events/count")
    @Operation(summary = "Count events", description = "Returns the total number of events with optional aerodrome filter")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "Event count",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON))
    })
    public Response countEvents(
            @Parameter(description = "Aerodrome ICAO filter", example = "LPPT")
            @QueryParam("aerodrome") String aerodrome) {

        long count = aerodrome != null && !aerodrome.isBlank()
                ? eventRepository.countByAerodrome(aerodrome)
                : eventRepository.count();
        return Response.ok(Map.of("count", count)).build();
    }
}
