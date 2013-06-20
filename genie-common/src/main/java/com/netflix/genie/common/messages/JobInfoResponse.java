/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.common.messages;

import java.util.Arrays;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.JobInfoElement;

/**
 * Represents the response from the Jobs REST resource.
 *
 * @author skrishnan
 * @author bmundlapudi
 */
@XmlRootElement(name = "response")
public class JobInfoResponse extends BaseResponse {
    private static final long serialVersionUID = -1L;

    private String message;
    private JobInfoElement[] jobs;

    /**
     * Constructor to use if there is an exception.
     *
     * @param error
     *            CloudServiceException for this response
     */
    public JobInfoResponse(CloudServiceException error) {
        super(error);
    }

    /**
     * Constructor.
     */
    public JobInfoResponse() {
    }

    /**
     * Get the human-readable message for this response.
     *
     * @return human-readable message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the human-readable message for this response.
     *
     * @param message
     *            human-readable message
     */
    @XmlElement(name = "message")
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Return a list of jobs for this response.
     *
     * @return array of jobInfo elements for this response
     */
    @XmlElementWrapper(name = "jobs")
    @XmlElement(name = "jobInfo")
    public JobInfoElement[] getJobs() {
        if (jobs == null) {
            return null;
        } else {
            return Arrays.copyOf(jobs, jobs.length);
        }
    }

    /**
     * Sets list of jobs for this response.
     *
     * @param inJobs
     *            an array of jobInfo elements for this response
     */
    public void setJobs(JobInfoElement[] inJobs) {
        if (inJobs == null) {
            this.jobs = null;
        } else {
            this.jobs = Arrays.copyOf(inJobs, inJobs.length);
        }
    }

    /**
     * Set an individual job for this response.
     *
     * @param job
     *            an individual jobInfo element for this response
     */
    public void setJob(JobInfoElement job) {
        setJobs(new JobInfoElement[] {job});
    }
}
