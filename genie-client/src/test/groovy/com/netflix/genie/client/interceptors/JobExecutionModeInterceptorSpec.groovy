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
package com.netflix.genie.client.interceptors


import okhttp3.Interceptor
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link JobExecutionModeInterceptor}.
 *
 * @author tgianos
 */
class JobExecutionModeInterceptorSpec extends Specification {

    @Unroll
    def "Calling intercept with embedded forced = #forceEmbedded and agent force = #forceAgent behaves as expected"() {
        def interceptor = new JobExecutionModeInterceptor(
            { forceEmbedded },
            { forceAgent }
        )
        def chain = Mock(Interceptor.Chain)
        def request = new Request.Builder()
            .post(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080" + JobExecutionModeInterceptor.JOBS_API)
            .build()
        def response = new Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .message("blah")
            .code(200)
            .build()

        when:
        interceptor.intercept(chain)

        then:
        1 * chain.request() >> request
        1 * chain.proceed(
            {
                verifyAll(it, Request) {
                    def headers = it.headers()
                    if (forceEmbedded) {
                        headers.get(JobExecutionModeInterceptor.FORCE_EMBEDDED_EXECUTION_HEADER_NAME) != null
                        headers.get(JobExecutionModeInterceptor.FORCE_EMBEDDED_EXECUTION_HEADER_NAME) == JobExecutionModeInterceptor.TRUE_STRING
                        headers.get(JobExecutionModeInterceptor.FORCE_AGENT_EXECUTION_HEADER_NAME) == null
                    } else if (forceAgent) {
                        headers.get(JobExecutionModeInterceptor.FORCE_EMBEDDED_EXECUTION_HEADER_NAME) == null
                        headers.get(JobExecutionModeInterceptor.FORCE_AGENT_EXECUTION_HEADER_NAME) != null
                        headers.get(JobExecutionModeInterceptor.FORCE_AGENT_EXECUTION_HEADER_NAME) == JobExecutionModeInterceptor.TRUE_STRING
                    } else {
                        headers.get(JobExecutionModeInterceptor.FORCE_EMBEDDED_EXECUTION_HEADER_NAME) == null
                        headers.get(JobExecutionModeInterceptor.FORCE_AGENT_EXECUTION_HEADER_NAME) == null
                    }
                }
            }
        ) >> response

        where:
        forceEmbedded | forceAgent
        true          | false
        true          | true
        false         | true
        false         | false
    }

    @Unroll
    def "Non new job request #request is ignored by the interceptor"() {
        def interceptor = new JobExecutionModeInterceptor(
            { true },
            { true }
        )
        def chain = Mock(Interceptor.Chain)

        when:
        interceptor.intercept(chain)

        then:
        1 * chain.request() >> request
        1 * chain.proceed(request)

        where:
        request      | _
        new Request.Builder()
            .post(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080/api/v3/applications")
            .build() | _
        new Request.Builder()
            .post(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080/api/v3/clusters")
            .build() | _
        new Request.Builder()
            .post(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080/api/v3/commands")
            .build() | _
        new Request.Builder()
            .post(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080/api/v3/jobs/" + UUID.randomUUID().toString())
            .build() | _
        new Request.Builder()
            .get()
            .url("http://localhost:8080/api/v3/jobs")
            .build() | _
        new Request.Builder()
            .put(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080/api/v3/jobs")
            .build() | _
        new Request.Builder()
            .patch(RequestBody.create(null, UUID.randomUUID().toString()))
            .url("http://localhost:8080/api/v3/jobs")
            .build() | _
        new Request.Builder()
            .get()
            .url("http://localhost:8080/api/v3/jobs")
            .build() | _
        new Request.Builder()
            .get()
            .url("http://localhost:8080/api/v3/jobs/" + UUID.randomUUID().toString())
            .build() | _
        new Request.Builder()
            .delete()
            .url("http://localhost:8080/api/v3/jobs/")
            .build() | _
        new Request.Builder()
            .head()
            .url("http://localhost:8080/api/v3/jobs/")
            .build() | _
    }
}
