package com.github.swim_developer.ed254.consumer.infrastructure.out.client;

import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionRequest;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionResponse;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface SubscriptionManagerRestClient {

    @POST
    @Path("/arrivalSequenceInformation/v1/subscriptions")
    @Retry(maxRetries = 3, delay = 1000, jitter = 500)
    @Timeout(5000)
    SubscriptionResponse createSubscription(SubscriptionRequest request);

    @GET
    @Path("/arrivalSequenceInformation/v1/subscriptions/{subscriptionId}")
    @Timeout(5000)
    SubscriptionResponse getSubscriptionDetails(@PathParam("subscriptionId") String subscriptionId);

    @PUT
    @Path("/arrivalSequenceInformation/v1/subscriptions/{subscriptionId}/pause")
    @Retry(maxRetries = 3, delay = 1000, jitter = 500)
    @Timeout(5000)
    SubscriptionResponse pauseSubscription(@PathParam("subscriptionId") String subscriptionId);

    @PUT
    @Path("/arrivalSequenceInformation/v1/subscriptions/{subscriptionId}/resume")
    @Retry(maxRetries = 3, delay = 1000, jitter = 500)
    @Timeout(5000)
    SubscriptionResponse resumeSubscription(@PathParam("subscriptionId") String subscriptionId);

    @DELETE
    @Path("/arrivalSequenceInformation/v1/subscriptions")
    @Retry(maxRetries = 2, delay = 2000)
    @Timeout(10000)
    void deleteSubscription(@QueryParam("subscriptionReference") String subscriptionReference);

    @PUT
    @Path("/arrivalSequenceInformation/v1/subscriptions/{subscriptionId}/renew")
    @Retry(maxRetries = 3, delay = 1000, jitter = 500)
    @Timeout(5000)
    SubscriptionResponse renewSubscription(@PathParam("subscriptionId") String subscriptionId);

    @GET
    @Path("/swim/v1/features")
    @Produces(MediaType.APPLICATION_XML)
    @Timeout(15000)
    String getFeatures(
            @QueryParam("typeName") String typeName,
            @QueryParam("aerodromeDesignator") String aerodromeDesignator,
            @QueryParam("messageType") String messageType,
            @QueryParam("validTime") String validTime,
            @QueryParam("startTime") String startTime,
            @QueryParam("endTime") String endTime,
            @QueryParam("startIndex") Integer startIndex,
            @QueryParam("count") Integer count
    );

    @POST
    @Path("/arrivalSequenceInformation/v1/problems")
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(5000)
    void communicateProblems(DataValidationResult validationResult);
}
