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
package com.netflix.genie.common.internal.dto.v4.files

import com.netflix.genie.test.suppliers.RandomSuppliers
import spock.lang.Specification

/**
 * Specifications for the {@link JobFileState} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class JobFileStateSpec extends Specification {

    def "Can construct and compare"() {
        when:
        def one = new JobFileState(UUID.randomUUID().toString(), RandomSuppliers.LONG.get(), null)
        def two = new JobFileState(UUID.randomUUID().toString(), RandomSuppliers.LONG.get(), UUID.randomUUID().toString())
        def three = new JobFileState(two.getPath(), two.getSize(), two.getMd5().orElse(UUID.randomUUID().toString()))
        def four = new JobFileState(one.getPath(), one.getSize(), null)

        then:
        one != two
        one != three
        one == four
        one.hashCode() != two.hashCode()
        one.hashCode() != three.hashCode()
        one.hashCode() == four.hashCode()
        one.toString() != two.toString()
        one.toString() != three.toString()
        one.toString() == four.toString()

        two == three
        two != four
        two.hashCode() == three.hashCode()
        two.hashCode() != four.hashCode()
        two.toString() == three.toString()
        two.toString() != four.toString()

        three != four
        three.hashCode() != four.hashCode()
        three.toString() != four.toString()
    }
}
