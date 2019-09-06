/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.client

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.client.exceptions.GenieClientException
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.search.JobSearchResult
import com.netflix.genie.common.util.GenieObjectMapper
import okhttp3.ResponseBody
import retrofit2.Response
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

/**
 * Specifications for {@link GenieClientUtils}.
 *
 * @author tgianos
 */
class GenieClientUtilsSpec extends Specification {

    @Unroll
    def "#body as search results returns expected results #expectedResults"() {
        setup:
        Response<JsonNode> response = Response.success(GenieObjectMapper.getMapper().readTree(body))

        when:
        def results = GenieClientUtils.parseSearchResultsResponse(response, "jobSearchResultList", JobSearchResult.class)

        then: "No exception is thrown and empty list is returned"
        noExceptionThrown()
        results == expectedResults
        where:
        body                                                                                                                                                                                                                                                                           | expectedResults
        "{}"                                                                                                                                                                                                                                                                           | []
        "[]"                                                                                                                                                                                                                                                                           | []
        "{\"_embedded\": []}"                                                                                                                                                                                                                                                          | []
        "{\"_embedded\": {\"notRight\": []}}"                                                                                                                                                                                                                                          | []
        "{\"_embedded\": {\"jobSearchResultList\": {}}}"                                                                                                                                                                                                                               | []
        "{\"_embedded\": {\"jobSearchResultList\": [{\"id\":\"1234\",\"name\":\"testJob\",\"user\":\"tgianos\",\"status\":\"SUCCEEDED\",\"started\":\"1970-01-01T00:00:50Z\",\"finished\":\"1970-01-01T00:00:52Z\",\"clusterName\":null,\"commandName\":null,\"runtime\":\"PT2S\"}]}}" | [new JobSearchResult("1234", "testJob", "tgianos", JobStatus.SUCCEEDED, Instant.ofEpochSecond(50), Instant.ofEpochSecond(52), null, null)]
    }

    def "Unsuccessful search result request throws client exception"() {
        Response<JsonNode> response = Response.error(500, Mock(ResponseBody))

        when:
        GenieClientUtils.parseSearchResultsResponse(response, "jobSearchResultList", JobSearchResult.class)

        then:
        thrown(GenieClientException)
    }
}
