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

import com.google.common.collect.ImmutableSet
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ClusterCriteria
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.internal.dto.v4.Cluster
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.properties.ScriptLoadBalancerProperties
import com.netflix.genie.web.services.impl.GenieFileTransferService
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.apache.commons.lang3.StringUtils
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.core.env.Environment
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Paths
import java.time.Instant
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
    def clustersGood = Sets.newHashSet(
        new Cluster(
            "2",
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                "a",
                "b",
                "c",
                ClusterStatus.UP
            ).build()
        ),
        new Cluster(
            "0",
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                "d",
                "e",
                "f",
                ClusterStatus.UP
            ).build()
        ),
        new Cluster(
            "1",
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                "g",
                "h",
                "i",
                ClusterStatus.UP
            ).build()
        )
    )

    @Shared
    def clustersBad = Sets.newHashSet(
        new Cluster(
            "3",
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                "j",
                "k",
                "l",
                ClusterStatus.UP
            ).build()
        ),
        new Cluster(
            "4",
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                "m",
                "n",
                "o",
                ClusterStatus.UP
            ).build()
        )
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
        this.executor.setCorePoolSize(2)
        this.executor.initialize()
    }

    def cleanupSpec() {
        this.executor.shutdown()
    }

    @Unroll
    def "Can select cluster using #type for script #file"() {
        def scheduler = Mock(TaskScheduler)
        def environment = Mock(Environment)
        def fileTransferService = Mock(GenieFileTransferService)
        def registry = Mock(MeterRegistry)
        def updateTimer = Mock(io.micrometer.core.instrument.Timer)
        def selectTimer = Mock(io.micrometer.core.instrument.Timer)
        def destDir = StringUtils.substringBeforeLast(file, "/")

        when: "Constructed"
        def loadBalancer = new ScriptLoadBalancer(
            this.executor,
            scheduler,
            fileTransferService,
            environment,
            GenieObjectMapper.getMapper(),
            registry
        )

        then:
        1 * environment.getProperty(
            ScriptLoadBalancerProperties.REFRESH_RATE_PROPERTY,
            Long.class,
            300_000L
        ) >> 300_000L
        1 * scheduler.scheduleWithFixedDelay(_ as Runnable, 300_000L)

        when: "Try to select after before update"
        def cluster = loadBalancer.selectCluster(this.clustersGood, this.jobRequest)

        then: "Should skip running script and do nothing"
        cluster == null
        1 * registry.timer(
            ScriptLoadBalancer.SELECT_TIMER_NAME,
            ImmutableSet.of(Tag.of(MetricsConstants.TagKeys.STATUS, ScriptLoadBalancer.STATUS_TAG_NOT_CONFIGURED))
        ) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "refresh is called but fails"
        loadBalancer.refresh()

        then: "Metrics are recorded"
        1 * environment.getProperty(ScriptLoadBalancerProperties.TIMEOUT_PROPERTY, Long.class, _ as Long) >> 5_000L
        1 * environment.getProperty(ScriptLoadBalancerProperties.SCRIPT_FILE_SOURCE_PROPERTY) >> null
        1 * registry.timer(
            ScriptLoadBalancer.UPDATE_TIMER_NAME,
            ImmutableSet.of(
                Tag.of(MetricsConstants.TagKeys.STATUS, ScriptLoadBalancer.STATUS_TAG_FAILED),
                Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, IllegalStateException.class.getName())
            )
        ) >> updateTimer
        1 * updateTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Try to select after failed update"
        cluster = loadBalancer.selectCluster(this.clustersGood, this.jobRequest)

        then: "Should skip running script and do nothing"
        cluster == null
        1 * registry.timer(
            ScriptLoadBalancer.SELECT_TIMER_NAME,
            ImmutableSet.of(
                Tag.of(MetricsConstants.TagKeys.STATUS, ScriptLoadBalancer.STATUS_TAG_NOT_CONFIGURED)
            )
        ) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Call refresh again"
        loadBalancer.refresh()

        then: "Refresh successfully configures the script"
        1 * environment.getProperty(ScriptLoadBalancerProperties.TIMEOUT_PROPERTY, Long.class, _ as Long) >> 5_000L
        1 * environment.getProperty(ScriptLoadBalancerProperties.SCRIPT_FILE_SOURCE_PROPERTY) >> file
        1 * environment.getProperty(ScriptLoadBalancerProperties.SCRIPT_FILE_DESTINATION_PROPERTY) >> destDir
        1 * fileTransferService.getFile(file, file)
        1 * registry.timer(
            ScriptLoadBalancer.UPDATE_TIMER_NAME,
            ImmutableSet.of(Tag.of(MetricsConstants.TagKeys.STATUS, ScriptLoadBalancer.STATUS_TAG_OK))
        ) >> updateTimer
        1 * updateTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Script is compiled after refresh. select is called again"
        cluster = loadBalancer.selectCluster(this.clustersGood, this.jobRequest)

        then: "Can successfully find a cluster"
        cluster != null
        cluster.getId() == "1"
        1 * registry.timer(
            ScriptLoadBalancer.SELECT_TIMER_NAME,
            ImmutableSet.of(Tag.of(MetricsConstants.TagKeys.STATUS, ScriptLoadBalancer.STATUS_TAG_FOUND))
        ) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        when: "Script is called with unhandled clusters"
        cluster = loadBalancer.selectCluster(this.clustersBad, this.jobRequest)

        then: "Can't find a cluster"
        cluster == null
        1 * registry.timer(
            ScriptLoadBalancer.SELECT_TIMER_NAME,
            ImmutableSet.of(Tag.of(MetricsConstants.TagKeys.STATUS, ScriptLoadBalancer.STATUS_TAG_NOT_FOUND))
        ) >> selectTimer
        1 * selectTimer.record(_ as Long, TimeUnit.NANOSECONDS)

        where:
        type         | file
        "JavaScript" | Paths.get(this.class.getResource("loadBalance.js").file).toUri().toString()
        "Groovy"     | Paths.get(this.class.getResource("loadBalance.groovy").file).toUri().toString()
    }
}
