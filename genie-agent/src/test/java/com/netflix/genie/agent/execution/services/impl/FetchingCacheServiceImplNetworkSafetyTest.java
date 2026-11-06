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
package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.URI;
import java.nio.file.Path;

/**
 * Tests for {@link FetchingCacheServiceImpl#rejectUnsafeNetworkTarget(URI)}.
 *
 * Regression tests for the SSRF vulnerability reported in GH #1260: an {@code http(s)} setupFile,
 * config, or dependency URI must not be able to reach a loopback, link-local (including the
 * {@code 169.254.169.254} cloud metadata address), or private address on the Agent's behalf.
 *
 * @author roridemonslayer
 * @since 4.4.0
 */
class FetchingCacheServiceImplNetworkSafetyTest {

    private FetchingCacheServiceImpl cache;

    @BeforeEach
    void setUp(@TempDir final Path temporaryFolder) throws Exception {
        final ArgumentDelegates.CacheArguments cacheArguments = Mockito.mock(ArgumentDelegates.CacheArguments.class);
        Mockito.when(cacheArguments.getCacheDirectory()).thenReturn(temporaryFolder.toFile());
        cache = new FetchingCacheServiceImpl(
            Mockito.mock(ResourceLoader.class),
            cacheArguments,
            Mockito.mock(FileLockFactory.class),
            new ThreadPoolTaskExecutor()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        "http://127.0.0.1:8080/admin",
        "https://localhost/admin",
        "http://10.0.0.5/internal-api",
        "http://172.16.0.5/internal-api",
        "http://192.168.1.5/internal-api",
        "http://0.0.0.0/",
        "http://[::1]/admin"
    })
    void rejectsInternalAndLoopbackTargets(final String uri) {
        Assertions
            .assertThatThrownBy(() -> cache.rejectUnsafeNetworkTarget(new URI(uri)))
            .isInstanceOf(DownloadException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // Literal IPs only - resolving a real hostname would make this test depend on live DNS/network
        // access, which isn't guaranteed in a CI/build sandbox. 198.51.100.0/24 is reserved for
        // documentation/examples (RFC 5737) and is neither private, loopback, nor link-local.
        "http://198.51.100.7/setup.sh",
        "https://198.51.100.7/setup.sh"
    })
    void allowsPublicHttpTargets(final String uri) throws Exception {
        Assertions.assertThatCode(() -> cache.rejectUnsafeNetworkTarget(new URI(uri))).doesNotThrowAnyException();
    }

    @Test
    void leavesNonNetworkSchemesAlone() throws Exception {
        // file: makes no outbound network request, so it isn't this check's concern - see the
        // ResourceUriValidator (genie-web) test for why file: is rejected earlier, at REST intake, instead.
        Assertions
            .assertThatCode(() -> cache.rejectUnsafeNetworkTarget(new URI("file:///etc/passwd")))
            .doesNotThrowAnyException();
    }
}
