/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.common.external.dtos.v4;

/**
 * Possible archival statuses for a Job.
 *
 * @author mprimi
 * @since 4.0.0
 */
public enum ArchiveStatus {

    /**
     * Files will be uploaded after the job finishes.
     */
    PENDING,

    /**
     * Files were archived successfully.
     */
    ARCHIVED,

    /**
     * Archival of files failed, files are not archived.
     */
    FAILED,

    /**
     * Archiving was disabled, files are not archived.
     */
    DISABLED,

    /**
     * No files were archived because no files were created.
     * i.e., job never reached the point where a directory is created.
     */
    NO_FILES,

    /**
     * Archive status is unknown.
     */
    UNKNOWN,
}
