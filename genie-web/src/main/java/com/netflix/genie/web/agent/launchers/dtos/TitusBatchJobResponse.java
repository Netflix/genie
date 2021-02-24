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
package com.netflix.genie.web.agent.launchers.dtos;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Titus job response POJO.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class TitusBatchJobResponse {
    private String id;
    private Integer statusCode;
    private String message;

    /**
     * Get the ID of the Titus job.
     *
     * @return The ID
     */
    public Optional<String> getId() {
        return Optional.ofNullable(this.id);
    }

    /**
     * Set the id of the titus job.
     *
     * @param id The new id
     */
    public void setId(@Nullable final String id) {
        this.id = StringUtils.isNotBlank(id) ? id : null;
    }

    /**
     * Get the status code if there was one.
     *
     * @return The status code wrapped in {@link Optional} else {@link Optional#empty()}
     */
    public Optional<Integer> getStatusCode() {
        return Optional.ofNullable(this.statusCode);
    }

    /**
     * Set the status code.
     *
     * @param statusCode The new status code
     */
    public void setStatusCode(@Nullable final Integer statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * Get the message if there was one.
     *
     * @return The message wrapped in {@link Optional} else {@link Optional#empty()}
     */
    public Optional<String> getMessage() {
        return Optional.ofNullable(this.message);
    }

    /**
     * Set the message if there was one.
     *
     * @param message The new message
     */
    public void setMessage(@Nullable final String message) {
        this.message = message;
    }
}
