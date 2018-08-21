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

package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.services.KillService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Implementation of {@link KillService}.
 *
 * @author standon
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
class KillServiceImpl implements KillService {

    private final ApplicationEventPublisher applicationEventPublisher;


    KillServiceImpl(
        final ApplicationEventPublisher applicationEventPublisher
    ) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill(final KillSource killSource) {

        UserConsole.getLogger().info("Job kill requested (source: {})", killSource.name());

        log.debug("Emitting kill event");

        applicationEventPublisher.publishEvent(
            new KillEvent(killSource)
        );

        log.debug("Kill event published");
    }
}
