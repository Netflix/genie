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

import com.wordnik.swagger.config.ConfigFactory;
import com.wordnik.swagger.model.ApiInfo;
import javax.servlet.http.HttpServlet;
import scala.Option;

/**
 * Simple bootstrap class for Swagger startup/configuration.
 *
 * @author tgianos
 */
public class GenieSwaggerBootstrap extends HttpServlet {

    static {
        final Option<ApiInfo> info = Option.apply(new ApiInfo(
                "Genie REST API", /* title */
                "See our <a href=\"http://netflix.github.io/genie\">GitHub Page</a> "
                        + "for more information. <br/>Post to our "
                        + "<a href=\"https://groups.google.com/d/forum/genie-hadoop\">"
                        + "Google Group</a> with any questions.<br/>"
                        + "Post any issues found "
                        + "<a href=\"https://github.com/Netflix/genie/issues\">here</a>.<br/>"
                        + "For more documentation see the "
                        + "<a href=\"https://github.com/Netflix/genie/wiki\">wiki</a>.<br/>",
                null, /* TOS URL */
                null, /* Contact */
                "Apache 2.0", /* license */
                "http://www.apache.org/licenses/LICENSE-2.0" /* license URL */
        ));

        ConfigFactory.config().setInfo(info);
    }
}
