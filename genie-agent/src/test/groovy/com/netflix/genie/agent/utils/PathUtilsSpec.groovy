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

package com.netflix.genie.agent.utils

import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.nio.file.Paths

class PathUtilsSpec extends Specification {

    static File jobDirectory = new File("/tmp/genie/job/1234")
    static String jobDirectoryStr = jobDirectory.getAbsolutePath()
    static Path entityDirPath = Paths.get("/tmp", "entity")
    static String entityDirPathStr = entityDirPath.toString()

    @Unroll
    def "ComposePath #expectedPath"(String expectedPath, String baseDir, String... children) {
        expect:
        expectedPath == PathUtils.composePath(new File(baseDir), children).toString()
        expectedPath == PathUtils.composePath(new File(baseDir).toPath(), children).toString()

        where:
        expectedPath | baseDir | children
        "/foo/bar"   | "/"     | ["foo", "bar"]

    }

    @Unroll
    def "EntitiesDirectoryPath #expectedPath"(Closure<Path> closure, String entityId, String expectedPath) {
        expect:
        expectedPath == closure(jobDirectory, entityId).toString()

        where:
        closure                                | entityId   | expectedPath
        PathUtils.&jobApplicationDirectoryPath | "my-app"   | jobDirectoryStr + "/genie/applications/my-app"
        PathUtils.&jobClusterDirectoryPath     | "my-clstr" | jobDirectoryStr + "/genie/cluster/my-clstr"
        PathUtils.&jobCommandDirectoryPath     | "my-cmd"   | jobDirectoryStr + "/genie/command/my-cmd"
    }

    @Unroll
    def "Dependencies and Configs Path #expectedPath"(Closure<Path> closure, Path entityDirectory, String expectedPath) {
        expect:
        expectedPath == closure(entityDirectory).toString()

        where:
        closure                              | entityDirectory | expectedPath
        PathUtils.&jobEntityDependenciesPath |  entityDirPath  | entityDirPathStr + "/dependencies"
        PathUtils.&jobEntityConfigPath       |  entityDirPath  | entityDirPathStr + "/config"
    }

    @Unroll
    def "Other paths: #expectedPath"(Closure<Path> closure, String expectedPath) {
        expect:
        expectedPath == closure(jobDirectory).toString()

        where:
        closure                              | expectedPath
        PathUtils.&jobGenieDirectoryPath     | jobDirectoryStr + "/genie"
        PathUtils.&jobGenieLogsDirectoryPath | jobDirectoryStr + "/genie/logs"
        PathUtils.&jobStdOutPath             | jobDirectoryStr + "/stdout"
        PathUtils.&jobStdErrPath             | jobDirectoryStr + "/stderr"
    }
}
