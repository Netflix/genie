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
package com.netflix.genie.client;

/**
 * Aggregate class for all the integration tests for the clients.
 * <p>
 * NOTE: This is done so that the single Genie test container can be reused throughout the tests and save time.
 * The way the classes extend from each other gives it ordering that {@literal @Nested} didn't seem to accomplish.
 * Since resources attached to jobs can't be deleted executing the job test(s) last makes it so that the previous
 * tests can maintain their functionality without completely re-writing them.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GenieClientIntegrationTest extends JobClientIntegrationTest {
}
