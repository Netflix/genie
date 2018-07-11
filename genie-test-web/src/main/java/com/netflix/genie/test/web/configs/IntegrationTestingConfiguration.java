/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.test.web.configs;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;

/**
 * Configuration overrides for integration tests.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@Profile(
    {
        "ci",
        "integration"
    }
)
public class IntegrationTestingConfiguration {

    private File jobsDir;

    /**
     * Returns a temporary directory as the jobs resource.
     *
     * @return The job dir as a resource.
     */
    @Bean
    public Resource jobsDir() {
        this.jobsDir = Files.createTempDir();
        if (!this.jobsDir.exists() && !this.jobsDir.mkdirs()) {
            throw new IllegalArgumentException("Unable to create directories: " + this.jobsDir);
        }

        String jobsDirPath = this.jobsDir.getAbsolutePath();
        final String slash = "/";
        if (!jobsDirPath.endsWith(slash)) {
            jobsDirPath = jobsDirPath + slash;
        }

        return new FileSystemResource(jobsDirPath);
    }

    /**
     * Get rid of the directories created by the system temporarily.
     *
     * @throws IOException when unable to delete directory
     */
    @PreDestroy
    public void cleanup() throws IOException {
        if (this.jobsDir != null) {
            FileUtils.deleteDirectory(this.jobsDir);
        }
    }
}
