package com.github.swim_developer.ed254.consumer.infrastructure.in.rest;

import com.github.swim_developer.ed254.consumer.infrastructure.out.mapper.Ed254ConsumerMapper;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionDTO;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoSubscriptionStore;
import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.framework.consumer.infrastructure.out.config.subscription.AbstractSubscriptionConfigParser;
import com.github.swim_developer.ed254.consumer.application.port.in.ManageSubscriptionPort;
import static com.github.swim_developer.framework.consumer.infrastructure.in.rest.ConsumerRestResponses.*;
import com.github.swim_developer.framework.application.model.SubscriptionStatusUpdate;
import com.github.swim_developer.framework.domain.model.SubscriptionStatus;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.Map;

@Slf4j
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "SWIM ED-254 Consumer Subscriptions", description = "Subscription and topic management")
public class ConsumerSubscriptionResource {

    private final MongoSubscriptionStore subscriptionRepository;
    private final Ed254ConsumerMapper mapper;
    private final AbstractSubscriptionConfigParser<?> configParser;
    private final ManageSubscriptionPort lifecycleService;

    @Inject
    public ConsumerSubscriptionResource(MongoSubscriptionStore subscriptionRepository,
                                        Ed254ConsumerMapper mapper,
                                        AbstractSubscriptionConfigParser<?> configParser,
                                        ManageSubscriptionPort lifecycleService) {
        this.subscriptionRepository = subscriptionRepository;
        this.mapper = mapper;
        this.configParser = configParser;
        this.lifecycleService = lifecycleService;
    }

    @GET
    @Path("/subscriptions")
    @Operation(summary = "List all subscriptions", description = "Retrieves all subscriptions from MongoDB regardless of status")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "List of subscriptions",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = SubscriptionDTO.class, type = SchemaType.ARRAY)))
    })
    public Response listAllSubscriptions() {
        List<Subscription> subscriptions = subscriptionRepository.findAllSubscriptions();
        List<SubscriptionDTO> dtos = subscriptions.stream()
                .map(mapper::toDTO)
                .toList();
        return Response.ok(dtos).build();
    }

    @GET
    @Path("/subscriptions/active")
    @Operation(summary = "List active subscriptions", description = "Retrieves only subscriptions with status ACTIVE")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "List of active subscriptions",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = SubscriptionDTO.class, type = SchemaType.ARRAY)))
    })
    public Response listActiveSubscriptions() {
        List<Subscription> subscriptions = subscriptionRepository.findActiveSubscriptions();
        List<SubscriptionDTO> dtos = subscriptions.stream()
                .map(mapper::toDTO)
                .toList();
        return Response.ok(dtos).build();
    }

    @POST
    @Path("/subscriptions")
    @Operation(summary = "Create subscription", description = "Creates a new manual subscription and registers AMQP consumer")
    @APIResponses(value = {
            @APIResponse(responseCode = "201", description = "Subscription created",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = SubscriptionDTO.class))),
            @APIResponse(responseCode = "400", description = "Invalid request"),
            @APIResponse(responseCode = "409", description = "Subscription already exists"),
            @APIResponse(responseCode = "503", description = "Subscription Manager unavailable")
    })
    public Response createSubscription(
            @RequestBody(description = "Subscription details", required = true,
                    content = @Content(schema = @Schema(implementation = DesiredSubscription.class)))
            DesiredSubscription request) {

        if (request.destinationAerodrome() == null || request.destinationAerodrome().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "At least one destinationAerodrome is required"))
                    .build();
        }

        try {
            Subscription document = lifecycleService.createSubscription(request);
            SubscriptionDTO dto = mapper.toDTO(document);
            log.info("Created manual subscription: {}", document.getSubscriptionId());
            return created(dto);

        } catch (Exception e) {
            log.error("Failed to create subscription", e);
            return serviceUnavailable("Failed to create subscription: " + e.getMessage());
        }
    }

    @DELETE
    @Path("/subscriptions/{subscriptionId}")
    @Operation(summary = "Delete subscription", description = "Deletes a subscription and unregisters AMQP consumer")
    @APIResponses(value = {
            @APIResponse(responseCode = "204", description = "Subscription deleted"),
            @APIResponse(responseCode = "404", description = "Subscription not found"),
            @APIResponse(responseCode = "503", description = "Subscription Manager unavailable")
    })
    public Response deleteSubscription(
            @Parameter(description = "Subscription ID", required = true, example = "sub-12345")
            @PathParam("subscriptionId") String subscriptionId) {

        try {
            lifecycleService.deleteSubscriptionById(subscriptionId);
            log.info("Deleted subscription: {}", subscriptionId);
            return noContent();

        } catch (IllegalArgumentException e) {
            return notFound(e.getMessage());
        } catch (Exception e) {
            log.error("Failed to delete subscription: {}", subscriptionId, e);
            return serviceUnavailable("Failed to delete subscription: " + e.getMessage());
        }
    }

    @PUT
    @Path("/subscriptions/{subscriptionId}")
    @Operation(summary = "Update subscription status", description = "Pauses or resumes a subscription (ACTIVE/PAUSED)")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "Subscription updated",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = SubscriptionDTO.class))),
            @APIResponse(responseCode = "400", description = "Invalid status"),
            @APIResponse(responseCode = "404", description = "Subscription not found"),
            @APIResponse(responseCode = "503", description = "Subscription Manager unavailable")
    })
    public Response updateSubscriptionStatus(
            @Parameter(description = "Subscription ID", required = true, example = "sub-12345")
            @PathParam("subscriptionId") String subscriptionId,
            @RequestBody(description = "New status (ACTIVE or PAUSED)", required = true,
                    content = @Content(schema = @Schema(implementation = SubscriptionStatusUpdate.class)))
            SubscriptionStatusUpdate statusUpdate) {

        if (statusUpdate == null || statusUpdate.subscriptionStatus() == null) {
            return badRequest("subscriptionStatus is required");
        }

        String newStatus = statusUpdate.subscriptionStatus().toUpperCase();
        if (!SubscriptionStatus.ACTIVE.name().equals(newStatus)
                && !SubscriptionStatus.PAUSED.name().equals(newStatus)) {
            return badRequest("subscriptionStatus must be ACTIVE or PAUSED");
        }

        try {
            Subscription document;
            if (newStatus.equals(SubscriptionStatus.PAUSED.name())) {
                document = lifecycleService.pauseSubscription(subscriptionId);
            } else {
                document = lifecycleService.resumeSubscription(subscriptionId);
            }

            SubscriptionDTO dto = mapper.toDTO(document);
            log.info("Updated subscription {} status to {}", subscriptionId, newStatus);
            return ok(dto);

        } catch (IllegalArgumentException e) {
            return notFound(e.getMessage());
        } catch (Exception e) {
            log.error("Failed to update subscription: {}", subscriptionId, e);
            return serviceUnavailable("Failed to update subscription: " + e.getMessage());
        }
    }

    @GET
    @Path("/topics")
    @Operation(summary = "List configured topics", description = "Retrieves topics configured via ConfigMap (ed254.subscriptions)")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "List of configured topics",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = DesiredSubscription.class, type = SchemaType.ARRAY)))
    })
    public Response listConfiguredTopics() {
        List<?> desiredSubscriptions = configParser.parseDesiredSubscriptions();
        return Response.ok(desiredSubscriptions).build();
    }
}
