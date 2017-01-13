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
package com.netflix.genie.web.configs

import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.properties.ZookeeperProperties
import org.junit.experimental.categories.Category
import spock.lang.Specification
/**
 * Tests for ZookeeperConfig.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Category(UnitTest.class)
class ZookeeperConfigSpec extends Specification {

    def zookeeperProperties = Mock(ZookeeperProperties)

    def "Can create curator framework factory bean"() {
        def config = new ZookeeperConfig()

        when:
        def factory = config.curatorFrameworkFactory(this.zookeeperProperties)

        then:
        factory != null
        1 * this.zookeeperProperties.getConnectionString() >> UUID.randomUUID().toString()
    }
}
