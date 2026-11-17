/*
 *
 *  Copyright 2026 Netflix, Inc.
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
package com.netflix.genie.web.util;

import com.netflix.genie.common.internal.dtos.ExecutionEnvironment;
import com.netflix.genie.common.internal.exceptions.unchecked.UnsafeResourceUriException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

/**
 * Unit tests for {@link ResourceUriValidator}.
 *
 * Regression tests for the SSRF/arbitrary-file-read vulnerability reported in GH #1260:
 * `setupFile`/`configs`/`dependencies` URIs submitted through the REST API must not be able to specify a
 * scheme (like `file:`) that a server-managed Agent could use to read files off its own host on behalf of
 * an untrusted caller.
 *
 * @author roridemonslayer
 * @since 4.4.0
 */
class ResourceUriValidatorTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "http://example.com/setup.sh",
        "https://example.com/setup.sh",
        "s3://my-bucket/setup.sh"
    })
    void allowedSchemesPass(final String uri) {
        Assertions.assertThatCode(() -> ResourceUriValidator.validate(Set.of(uri))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "file:///etc/passwd",
        "file:/etc/passwd",
        "classpath:some-internal-resource.txt",
        "ftp://example.com/setup.sh",
        "not-a-uri-at-all-just-a-bare-path",
        "gopher://internal-service:6379/_INFO"
    })
    void disallowedSchemesAreRejected(final String uri) {
        Assertions
            .assertThatThrownBy(() -> ResourceUriValidator.validate(Set.of(uri)))
            .isInstanceOf(UnsafeResourceUriException.class);
    }

    @Test
    void malformedUriIsRejected() {
        Assertions
            .assertThatThrownBy(() -> ResourceUriValidator.validate(Set.of("http://[invalid")))
            .isInstanceOf(UnsafeResourceUriException.class);
    }

    @Test
    void validExecutionEnvironmentPasses() {
        final ExecutionEnvironment resources = new ExecutionEnvironment(
            Set.of("s3://bucket/config.xml"),
            Set.of("http://example.com/dep.jar"),
            "https://example.com/setup.sh"
        );
        Assertions.assertThatCode(() -> ResourceUriValidator.validate(resources)).doesNotThrowAnyException();
    }

    @Test
    void executionEnvironmentWithUnsafeSetupFileIsRejected() {
        final ExecutionEnvironment resources = new ExecutionEnvironment(
            Set.of(),
            Set.of(),
            "file:///etc/passwd"
        );
        Assertions
            .assertThatThrownBy(() -> ResourceUriValidator.validate(resources))
            .isInstanceOf(UnsafeResourceUriException.class);
    }

    @Test
    void executionEnvironmentWithUnsafeConfigIsRejected() {
        final ExecutionEnvironment resources = new ExecutionEnvironment(
            Set.of("file:///etc/shadow"),
            Set.of(),
            null
        );
        Assertions
            .assertThatThrownBy(() -> ResourceUriValidator.validate(resources))
            .isInstanceOf(UnsafeResourceUriException.class);
    }

    @Test
    void executionEnvironmentWithUnsafeDependencyIsRejected() {
        final ExecutionEnvironment resources = new ExecutionEnvironment(
            Set.of(),
            Set.of("file:///etc/shadow"),
            null
        );
        Assertions
            .assertThatThrownBy(() -> ResourceUriValidator.validate(resources))
            .isInstanceOf(UnsafeResourceUriException.class);
    }

    @Test
    void executionEnvironmentWithNoSetupFilePasses() {
        final ExecutionEnvironment resources = new ExecutionEnvironment(Set.of(), Set.of(), null);
        Assertions.assertThatCode(() -> ResourceUriValidator.validate(resources)).doesNotThrowAnyException();
    }
}
