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
package com.netflix.genie.agent.utils.locks.impl

import com.netflix.genie.agent.execution.exceptions.LockException
import org.junit.Rule
import org.junit.rules.TemporaryFolder;
import spock.lang.Specification

/**
 * Specifications for the {@link FileLock} class.
 *
 * @author standon
 * @since 4.0.0
 */
class FileLockSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    def "Valid file returns a lock"() {
        FileLock lock
        when:
        lock = new FileLock(temporaryFolder.newFile())

        then:
        lock != null
    }

    def "Throws exception for bad file"() {
        when:
        new FileLock(null)

        then:
        thrown(LockException)

        when:
        new FileLock(new File(temporaryFolder.getRoot(), UUID.randomUUID().toString()))

        then:
        thrown(LockException)
    }
}
