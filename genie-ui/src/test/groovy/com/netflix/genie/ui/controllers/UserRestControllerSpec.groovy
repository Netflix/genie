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
package com.netflix.genie.ui.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

import javax.servlet.http.HttpServletRequest
import java.security.Principal

/**
 * Specifications for the {@link UserRestController} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class UserRestControllerSpec extends Specification {

    def "Can get user info"() {
        def controller = new UserRestController()
        def request = Mock(HttpServletRequest)
        def principal = Mock(Principal)
        def user = UUID.randomUUID().toString()
        JsonNode node

        when: "Principal is null"
        node = controller.getUserInfo(request)

        then: "Default user is returned"
        1 * request.getUserPrincipal() >> null
        node.size() == 1
        node.has(UserRestController.NAME_KEY)
        node.get(UserRestController.NAME_KEY).isTextual()
        node.get(UserRestController.NAME_KEY).textValue() == UserRestController.DEFAULT_USER

        when: "Principal is not null"
        node = controller.getUserInfo(request)

        then: "Default user is returned"
        1 * request.getUserPrincipal() >> principal
        1 * principal.getName() >> user
        node.size() == 1
        node.has(UserRestController.NAME_KEY)
        node.get(UserRestController.NAME_KEY).isTextual()
        node.get(UserRestController.NAME_KEY).textValue() == user
    }
}
