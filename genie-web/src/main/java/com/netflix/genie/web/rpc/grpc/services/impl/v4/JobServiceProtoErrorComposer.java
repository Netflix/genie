/*
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.genie.web.rpc.grpc.services.impl.v4;

import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException;
import com.netflix.genie.proto.ChangeJobStatusError;
import com.netflix.genie.proto.ChangeJobStatusResponse;
import com.netflix.genie.proto.ClaimJobError;
import com.netflix.genie.proto.ClaimJobResponse;
import com.netflix.genie.proto.JobSpecificationError;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdError;
import com.netflix.genie.proto.ReserveJobIdResponse;
import com.netflix.genie.web.properties.GRpcServerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintViolationException;
import java.util.Map;

/**
 * Utility/helper to map exceptions into protocol responses.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@Component
@Lazy
class JobServiceProtoErrorComposer {

    private static final String NO_ERROR_MESSAGE_PROVIDED = "Unknown error";

    private static final Map<Class<? extends Exception>, ReserveJobIdError.Type> RESERVE_JOB_ID_ERROR_MAP =
        ImmutableMap.<Class<? extends Exception>, ReserveJobIdError.Type>builder()
            .put(GenieConversionException.class, ReserveJobIdError.Type.INVALID_REQUEST)
            .put(GenieIdAlreadyExistsException.class, ReserveJobIdError.Type.ID_NOT_AVAILABLE)
            .build();

    private static final Map<Class<? extends Exception>, JobSpecificationError.Type> JOB_SPECIFICATION_ERROR_MAP =
        ImmutableMap.<Class<? extends Exception>, JobSpecificationError.Type>builder()
            .put(GenieJobNotFoundException.class, JobSpecificationError.Type.NO_JOB_FOUND)
            .put(GenieClusterNotFoundException.class, JobSpecificationError.Type.NO_CLUSTER_FOUND)
            .put(GenieCommandNotFoundException.class, JobSpecificationError.Type.NO_COMMAND_FOUND)
            .put(GenieApplicationNotFoundException.class, JobSpecificationError.Type.NO_APPLICATION_FOUND)
            .put(GenieJobSpecificationNotFoundException.class, JobSpecificationError.Type.NO_SPECIFICATION_FOUND)
            .put(ConstraintViolationException.class, JobSpecificationError.Type.INVALID_REQUEST)
            .build();

    private static final Map<Class<? extends Exception>, ClaimJobError.Type> CLAIM_JOB_ERROR_MAP =
        ImmutableMap.<Class<? extends Exception>, ClaimJobError.Type>builder()
            .put(GenieJobAlreadyClaimedException.class, ClaimJobError.Type.ALREADY_CLAIMED)
            .put(GenieJobNotFoundException.class, ClaimJobError.Type.NO_SUCH_JOB)
            .put(GenieInvalidStatusException.class, ClaimJobError.Type.INVALID_STATUS)
            .put(ConstraintViolationException.class, ClaimJobError.Type.INVALID_REQUEST)
            .build();

    private static final Map<Class<? extends Exception>, ChangeJobStatusError.Type> CHANGE_JOB_STATUS_ERROR_MAP =
        ImmutableMap.<Class<? extends Exception>, ChangeJobStatusError.Type>builder()
            .put(GenieJobNotFoundException.class, ChangeJobStatusError.Type.NO_SUCH_JOB)
            .put(GenieInvalidStatusException.class, ChangeJobStatusError.Type.INCORRECT_CURRENT_STATUS)
            .put(GeniePreconditionException.class, ChangeJobStatusError.Type.INVALID_REQUEST)
            .put(ConstraintViolationException.class, ChangeJobStatusError.Type.INVALID_REQUEST)
            .build();

    /**
     * Build a {@link ReserveJobIdResponse} out of the given {@link Exception}.
     *
     * @param e The server exception
     * @return The response
     */
    ReserveJobIdResponse toProtoReserveJobIdResponse(final Exception e) {
        return ReserveJobIdResponse.newBuilder()
            .setError(
                ReserveJobIdError.newBuilder()
                    .setMessage(getMessage(e))
                    .setType(getErrorType(e, RESERVE_JOB_ID_ERROR_MAP, ReserveJobIdError.Type.UNKNOWN))
            ).build();
    }

    /**
     * Build a {@link JobSpecificationResponse} out of the given {@link Exception}.
     *
     * @param e The server exception
     * @return The response
     */
    JobSpecificationResponse toProtoJobSpecificationResponse(final Exception e) {
        return JobSpecificationResponse.newBuilder()
            .setError(JobSpecificationError.newBuilder()
                .setMessage(getMessage(e))
                .setType(getErrorType(e, JOB_SPECIFICATION_ERROR_MAP, JobSpecificationError.Type.UNKNOWN))
            )
            .build();
    }

    /**
     * Build a {@link ClaimJobResponse} out of the given {@link Exception}.
     *
     * @param e The server exception
     * @return The response
     */
    ClaimJobResponse toProtoClaimJobResponse(final Exception e) {
        return ClaimJobResponse.newBuilder()
            .setSuccessful(false)
            .setError(
                ClaimJobError.newBuilder()
                    .setMessage(getMessage(e))
                    .setType(getErrorType(e, CLAIM_JOB_ERROR_MAP, ClaimJobError.Type.UNKNOWN))
            )
            .build();
    }

    /**
     * Build a {@link ChangeJobStatusResponse} out of the given {@link Exception}.
     *
     * @param e The server exception
     * @return The response
     */
    ChangeJobStatusResponse toProtoChangeJobStatusResponse(final Exception e) {
        return ChangeJobStatusResponse.newBuilder()
            .setSuccessful(false)
            .setError(
                ChangeJobStatusError.newBuilder()
                    .setMessage(getMessage(e))
                    .setType(getErrorType(e, CHANGE_JOB_STATUS_ERROR_MAP, ChangeJobStatusError.Type.UNKNOWN))
            )
            .build();
    }

    private static <T> T getErrorType(
        final Exception e,
        final Map<Class<? extends Exception>, T> typeMap,
        final T defaultValue
    ) {
        for (final Map.Entry<Class<? extends Exception>, T> typeMapEntry : typeMap.entrySet()) {
            if (typeMapEntry.getKey().isInstance(e)) {
                return typeMapEntry.getValue();
            }
        }
        return defaultValue;
    }

    private static String getMessage(final Exception e) {
        return e.getClass().getCanonicalName()
            + ":"
            + (e.getMessage() == null ? NO_ERROR_MESSAGE_PROVIDED : e.getMessage());
    }

}
