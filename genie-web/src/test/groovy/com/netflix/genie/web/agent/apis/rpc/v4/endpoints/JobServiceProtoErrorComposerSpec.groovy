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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints

import com.netflix.genie.common.exceptions.GeniePreconditionException
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.proto.ChangeJobStatusError
import com.netflix.genie.proto.ChangeJobStatusResponse
import com.netflix.genie.proto.ClaimJobError
import com.netflix.genie.proto.ClaimJobResponse
import com.netflix.genie.proto.ConfigureResponse
import com.netflix.genie.proto.HandshakeResponse
import com.netflix.genie.proto.JobSpecificationError
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.ReserveJobIdError
import com.netflix.genie.proto.ReserveJobIdResponse
import org.assertj.core.util.Sets
import spock.lang.Specification
import spock.lang.Unroll

import javax.validation.ConstraintViolation
import javax.validation.ConstraintViolationException

class JobServiceProtoErrorComposerSpec extends Specification {
    static final String MESSAGE = "some message"
    static final Set<ConstraintViolation> cvs = Sets.newHashSet()
    JobServiceProtoErrorComposer errorComposer

    def setup() {
        errorComposer = new JobServiceProtoErrorComposer();
    }

    @Unroll
    def "ToProtoReserveJobIdResponse for #exception.class.getSimpleName()"() {
        when:
        ReserveJobIdResponse response = errorComposer.toProtoReserveJobIdResponse(exception)

        then:
        response.hasError()
        response.getError().getType() == expectedErrorType
        response.getError().getMessage().contains(exception.class.getCanonicalName())
        response.getError().getMessage().contains(MESSAGE)

        where:
        exception                                  | expectedErrorType
        new GenieConversionException(MESSAGE)      | ReserveJobIdError.Type.INVALID_REQUEST
        new GenieIdAlreadyExistsException(MESSAGE) | ReserveJobIdError.Type.ID_NOT_AVAILABLE
        new IOException(MESSAGE)                   | ReserveJobIdError.Type.UNKNOWN
        new RuntimeException(MESSAGE)              | ReserveJobIdError.Type.UNKNOWN
    }

    @Unroll
    def "ToProtoJobSpecificationResponse for #exception.class.getSimpleName()"() {
        when:
        JobSpecificationResponse response = errorComposer.toProtoJobSpecificationResponse(exception)

        then:
        response.hasError()
        response.getError().getType() == expectedErrorType
        response.getError().getMessage().contains(MESSAGE)

        where:
        exception                                                                                     | expectedErrorType
        new GenieJobNotFoundException(MESSAGE)                                                        | JobSpecificationError.Type.NO_JOB_FOUND
        new GenieClusterNotFoundException(MESSAGE)                                                    | JobSpecificationError.Type.NO_CLUSTER_FOUND
        new GenieCommandNotFoundException(MESSAGE)                                                    | JobSpecificationError.Type.NO_COMMAND_FOUND
        new GenieApplicationNotFoundException(MESSAGE)                                                | JobSpecificationError.Type.NO_APPLICATION_FOUND
        new GenieJobSpecificationNotFoundException(MESSAGE)                                           | JobSpecificationError.Type.NO_SPECIFICATION_FOUND
        new ConstraintViolationException(MESSAGE, cvs)                                                | JobSpecificationError.Type.INVALID_REQUEST
        new IOException(MESSAGE)                                                                      | JobSpecificationError.Type.UNKNOWN
        new RuntimeException(MESSAGE)                                                                 | JobSpecificationError.Type.UNKNOWN
        new GenieJobResolutionException(MESSAGE, new GenieJobNotFoundException(MESSAGE))              | JobSpecificationError.Type.NO_JOB_FOUND
        new GenieJobResolutionException(MESSAGE, new GenieClusterNotFoundException(MESSAGE))          | JobSpecificationError.Type.NO_CLUSTER_FOUND
        new GenieJobResolutionException(MESSAGE, new GenieCommandNotFoundException(MESSAGE))          | JobSpecificationError.Type.NO_COMMAND_FOUND
        new GenieJobResolutionException(MESSAGE, new GenieApplicationNotFoundException(MESSAGE))      | JobSpecificationError.Type.NO_APPLICATION_FOUND
        new GenieJobResolutionException(MESSAGE, new GenieJobSpecificationNotFoundException(MESSAGE)) | JobSpecificationError.Type.NO_SPECIFICATION_FOUND
        new GenieJobResolutionException(MESSAGE, new GeniePreconditionException(MESSAGE))             | JobSpecificationError.Type.RESOLUTION_FAILED
        new GenieJobResolutionException(MESSAGE)                                                      | JobSpecificationError.Type.RESOLUTION_FAILED
        new GenieJobResolutionException(new Throwable(MESSAGE))                                       | JobSpecificationError.Type.RESOLUTION_FAILED
    }

    @Unroll
    def "ToProtoClaimJobResponse for #exception.class.getSimpleName()"() {
        when:
        ClaimJobResponse response = errorComposer.toProtoClaimJobResponse(exception)

        then:
        response.hasError()
        response.getError().getType() == expectedErrorType
        response.getError().getMessage().contains(exception.class.getCanonicalName())
        response.getError().getMessage().contains(MESSAGE)

        where:
        exception                                      | expectedErrorType
        new GenieJobAlreadyClaimedException(MESSAGE)   | ClaimJobError.Type.ALREADY_CLAIMED
        new GenieJobNotFoundException(MESSAGE)         | ClaimJobError.Type.NO_SUCH_JOB
        new GenieInvalidStatusException(MESSAGE)       | ClaimJobError.Type.INVALID_STATUS
        new ConstraintViolationException(MESSAGE, cvs) | ClaimJobError.Type.INVALID_REQUEST
        new IOException(MESSAGE)                       | ClaimJobError.Type.UNKNOWN
        new RuntimeException(MESSAGE)                  | ClaimJobError.Type.UNKNOWN
    }

    @Unroll
    def "ToProtoChangeJobStatusResponse for #exception.class.getSimpleName()"() {
        when:
        ChangeJobStatusResponse response = errorComposer.toProtoChangeJobStatusResponse(exception)

        then:
        response.hasError()
        response.getError().getType() == expectedErrorType
        response.getError().getMessage().contains(exception.class.getCanonicalName())
        response.getError().getMessage().contains(MESSAGE)

        where:
        exception                                      | expectedErrorType
        new GenieJobNotFoundException(MESSAGE)         | ChangeJobStatusError.Type.NO_SUCH_JOB
        new GenieInvalidStatusException(MESSAGE)       | ChangeJobStatusError.Type.INCORRECT_CURRENT_STATUS
        new GeniePreconditionException(MESSAGE)        | ChangeJobStatusError.Type.INVALID_REQUEST
        new ConstraintViolationException(MESSAGE, cvs) | ChangeJobStatusError.Type.INVALID_REQUEST
        new IOException(MESSAGE)                       | ChangeJobStatusError.Type.UNKNOWN
        new RuntimeException(MESSAGE)                  | ChangeJobStatusError.Type.UNKNOWN
    }

    @Unroll
    def "ToProtoHandshakeResponse for #exception.class.getSimpleName()"() {
        when:
        HandshakeResponse response = errorComposer.toProtoHandshakeResponse(exception)

        then:
        response.getType() == expectedErrorType
        response.getMessage().contains(exception.class.getCanonicalName())
        response.getMessage().contains(MESSAGE)

        where:
        exception                                      | expectedErrorType
        new GenieAgentRejectedException(MESSAGE)       | HandshakeResponse.Type.REJECTED
        new ConstraintViolationException(MESSAGE, cvs) | HandshakeResponse.Type.INVALID_REQUEST
        new RuntimeException(MESSAGE)                  | HandshakeResponse.Type.SERVER_ERROR
    }

    @Unroll
    def "ToProtoConfigureResponse for #exception.class.getSimpleName()"() {
        when:
        ConfigureResponse response = errorComposer.toProtoConfigureResponse(exception)

        then:
        response.getPropertiesMap().isEmpty()

        where:
        exception                     | _
        new RuntimeException(MESSAGE) | _
    }
}
