/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.services.loadbalancers.script

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.Cluster
import com.netflix.genie.common.dto.ClusterCriteria
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.util.GenieDateFormat
import com.netflix.genie.core.services.ClusterLoadBalancer
import com.netflix.genie.core.services.impl.GenieFileTransferService
import com.netflix.genie.core.util.MetricsConstants
import com.netflix.genie.test.categories.UnitTest
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
import org.apache.commons.lang.StringUtils
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.core.env.Environment
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

/**
 * Specifications for the ScriptLoadBalancer class.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Category(UnitTest.class)
class ScriptLoadBalancerSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    @Shared
    def mapper = new ObjectMapper().registerModule(new Jdk8Module())

    @Shared
    def clustersGood = Sets.newHashSet(
            new Cluster.Builder("a", "b", "c", ClusterStatus.UP).withId("2").build(),
            new Cluster.Builder("d", "e", "f", ClusterStatus.UP).withId("0").build(),
            new Cluster.Builder("g", "h", "i", ClusterStatus.UP).withId("1").build()
    )

    @Shared
    def clustersBad = Sets.newHashSet(
            new Cluster.Builder("j", "k", "l", ClusterStatus.UP).withId("3").build(),
            new Cluster.Builder("m", "n", "o", ClusterStatus.UP).withId("4").build()
    )

    @Shared
    def jobRequest = new JobRequest.Builder(
            "jobName",
            "jobUser",
            "jobVersion",
            Lists.newArrayList(
                    new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            ),
            Sets.newHashSet(UUID.randomUUID().toString())
    ).build()

    @Shared
    def executor = new ThreadPoolTaskExecutor()

    def setupSpec() {
        def iso8601 = new GenieDateFormat()
        iso8601.setTimeZone(TimeZone.getTimeZone("UTC"))
        this.mapper.setDateFormat(iso8601)

        this.executor.setCorePoolSize(2)
        this.executor.initialize()
    }

    def cleanupSpec() {
        this.executor.shutdown()
    }

    @Unroll
    def "Order should be #order"() {
        def loadBalancer = new ScriptLoadBalancer(
                Mock(AsyncTaskExecutor),
                Mock(TaskScheduler) {
                    1 * scheduleWithFixedDelay(_ as Runnable, 300_000L)
                },
                Mock(GenieFileTransferService),
                environment,
                Mock(ObjectMapper),
                Mock(Registry)
        )

        expect:
        loadBalancer.getOrder() == order

        where:
        environment | order
        Mock(Environment) {
            1 * getProperty(
                    ScriptLoadBalancer.SCRIPT_REFRESH_RATE_PROPERTY_KEY,
                    Long.class,
                    300_000L
            ) >> 300_000L
            1 * getProperty(
                    ScriptLoadBalancer.SCRIPT_LOAD_BALANCER_ORDER_PROPERTY_KEY,
                    Integer.class,
                    ClusterLoadBalancer.DEFAULT_ORDER
            ) >> ClusterLoadBalancer.DEFAULT_ORDER
        }           | ClusterLoadBalancer.DEFAULT_ORDER
        Mock(Environment) {
            1 * getProperty(
                    ScriptLoadBalancer.SCRIPT_REFRESH_RATE_PROPERTY_KEY,
                    Long.class,
                    300_000L
            ) >> 300_000L
            1 * getProperty(
                    ScriptLoadBalancer.SCRIPT_LOAD_BALANCER_ORDER_PROPERTY_KEY,
                    Integer.class,
                    _ as Integer
            ) >> 3
        }           | 3
    }

    @Unroll
    def "Can select cluster using #type for script #file"() {
        def scheduler = Mock(TaskScheduler)
        def environment = Mock(Environment)
        def fileTransferService = Mock(GenieFileTransferService)
        def registry = Mock(Registry)
        def updateId = Mock(Id)
        def selectId = Mock(Id)
        def updateTimer = Mock(Timer)
        def selectTimer = Mock(Timer)
        def destDir = StringUtils.substringBeforeLast(file, "/")

        when: "Constructed"
        def loadBalancer = new ScriptLoadBalancer(
                this.executor,
                scheduler,
                fileTransferService,
                environment,
                this.mapper,
                registry
        )

        then:
        1 * environment.getProperty(
                ScriptLoadBalancer.SCRIPT_REFRESH_RATE_PROPERTY_KEY,
                Long.class,
                300_000L
        ) >> 300_000L
        1 * environment.getProperty(
                ScriptLoadBalancer.SCRIPT_LOAD_BALANCER_ORDER_PROPERTY_KEY,
                Integer.class,
                _ as Integer
        ) >> 3
        1 * scheduler.scheduleWithFixedDelay(_ as Runnable, 300_000L)

        when: "Try to select after before update"
        def cluster = loadBalancer.selectCluster(this.clustersGood, this.jobRequest)

        then: "Should skip running script and do nothing"
        cluster == null
        1 * registry.createId(
                ScriptLoadBalancer.SELECT_TIMER_NAME,
                ImmutableMap.of(
                        MetricsConstants.TagKeys.STATUS,
                        ScriptLoadBalancer.STATUS_TAG_NOT_CONFIGURED
                )
        ) >> selectId
        1 * registry.timer(selectId) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "refresh is called but fails"
        loadBalancer.refresh()

        then: "Metrics are recorded"
        1 * environment.getProperty(ScriptLoadBalancer.SCRIPT_TIMEOUT_PROPERTY_KEY, Long.class, _ as Long) >> 5_000L
        1 * environment.getProperty(ScriptLoadBalancer.SCRIPT_FILE_SOURCE_PROPERTY_KEY) >> null
        1 * registry.createId(
                ScriptLoadBalancer.UPDATE_TIMER_NAME,
                ImmutableMap.of(
                        MetricsConstants.TagKeys.STATUS,
                        ScriptLoadBalancer.STATUS_TAG_FAILED,
                        MetricsConstants.TagKeys.EXCEPTION_CLASS,
                        IllegalStateException.class.getName()
                )
        ) >> updateId
        1 * registry.timer(updateId) >> updateTimer
        1 * updateTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Try to select after failed update"
        cluster = loadBalancer.selectCluster(this.clustersGood, this.jobRequest)

        then: "Should skip running script and do nothing"
        cluster == null
        1 * registry.createId(
                ScriptLoadBalancer.SELECT_TIMER_NAME,
                ImmutableMap.of(
                        MetricsConstants.TagKeys.STATUS,
                        ScriptLoadBalancer.STATUS_TAG_NOT_CONFIGURED
                )
        ) >> selectId
        1 * registry.timer(selectId) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Call refresh again"
        loadBalancer.refresh()

        then: "Refresh successfully configures the script"
        1 * environment.getProperty(ScriptLoadBalancer.SCRIPT_TIMEOUT_PROPERTY_KEY, Long.class, _ as Long) >> 5_000L
        1 * environment.getProperty(ScriptLoadBalancer.SCRIPT_FILE_SOURCE_PROPERTY_KEY) >> file
        1 * environment.getProperty(ScriptLoadBalancer.SCRIPT_FILE_DESTINATION_PROPERTY_KEY) >> destDir
        1 * fileTransferService.getFile(file, file)
        1 * registry.createId(
                ScriptLoadBalancer.UPDATE_TIMER_NAME,
                ImmutableMap.of(
                        MetricsConstants.TagKeys.STATUS,
                        ScriptLoadBalancer.STATUS_TAG_OK
                )
        ) >> updateId
        1 * registry.timer(updateId) >> updateTimer
        1 * updateTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Script is compiled after refresh. select is called again"
        cluster = loadBalancer.selectCluster(this.clustersGood, this.jobRequest)

        then: "Can successfully find a cluster"
        cluster != null
        cluster.getId().get() == "1"
        1 * registry.createId(
                ScriptLoadBalancer.SELECT_TIMER_NAME,
                ImmutableMap.of(
                        MetricsConstants.TagKeys.STATUS,
                        ScriptLoadBalancer.STATUS_TAG_FOUND
                )
        ) >> selectId
        1 * registry.timer(selectId) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Script is called with unhandled clusters"
        cluster = loadBalancer.selectCluster(this.clustersBad, this.jobRequest)

        then: "Can't find a cluster"
        cluster == null
        1 * registry.createId(
                ScriptLoadBalancer.SELECT_TIMER_NAME,
                ImmutableMap.of(
                        MetricsConstants.TagKeys.STATUS,
                        ScriptLoadBalancer.STATUS_TAG_NOT_FOUND
                )
        ) >> selectId
        1 * registry.timer(selectId) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        where:
        type         | file
        "JavaScript" | Paths.get(this.class.getResource("loadBalance.js").file).toUri().toString()
        "Groovy"     | Paths.get(this.class.getResource("loadBalance.groovy").file).toUri().toString()
    }
}
