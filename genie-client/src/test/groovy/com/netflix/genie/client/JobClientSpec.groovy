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

import com.netflix.genie.common.util.GenieObjectMapper
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link JobClient}.
 *
 * @author tgianos
 */
class JobClientSpec extends Specification {

    @Unroll
    def "#body as search results don't throw null pointer exception"() {
        setup:
        def server = new MockWebServer()
        server.enqueue(new MockResponse().setBody(body))
        server.start()
        def url = server.url("")
        def okHttpClient = new OkHttpClient.Builder().build()
        def retrofit = new Retrofit.Builder()
            .baseUrl(url)
            .client(okHttpClient)
            .addConverterFactory(JacksonConverterFactory.create(GenieObjectMapper.getMapper()))
            .build()
        def jobClient = new JobClient(retrofit, 5)

        when:
        def jobs = jobClient.getJobs()

        then: "No exception is thrown and empty list is returned"
        noExceptionThrown()
        jobs.isEmpty()

        cleanup:
        server.shutdown()

        where:
        body                                             | _
        "{}"                                             | _
        "[]"                                             | _
        "{\"_embedded\": []}"                            | _
        "{\"_embedded\": {\"notRight\": []}}"            | _
        "{\"_embedded\": {\"jobSearchResultList\": {}}}" | _
        "{\"_embedded\": {\"jobSearchResultList\": []}}" | _
    }
}
