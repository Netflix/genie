/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.startup;

import com.wordnik.swagger.config.*;
import com.wordnik.swagger.model.ApiInfo;
import javax.servlet.http.HttpServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Simple bootstrap class for Swagger startup/configuration.
 *
 * @author tgianos
 */
public class SwaggerBootstrap extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(SwaggerBootstrap.class);

    static {
        final Option<ApiInfo> info = Option.apply(new ApiInfo(
                "Genie", /* title */
                "See <a href=\"http://netflix.github.io/genie\">GitHub</a> for more information.",
                null, /* TOS URL */
                "bigdataplatform@netflix.com", /* Contact */
                "Apache 2.0", /* license */
                "http://www.apache.org/licenses/LICENSE-2.0" /* license URL */
        ));

        ConfigFactory.config().setInfo(info);
    }
}
