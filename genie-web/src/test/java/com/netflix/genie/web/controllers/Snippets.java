/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.controllers;

import com.netflix.genie.common.dto.Application;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpHeaders;
import org.springframework.restdocs.constraints.ConstraintDescriptions;
import org.springframework.restdocs.headers.HeaderDocumentation;
import org.springframework.restdocs.headers.ResponseHeadersSnippet;
import org.springframework.restdocs.hypermedia.HypermediaDocumentation;
import org.springframework.restdocs.hypermedia.LinksSnippet;
import org.springframework.restdocs.payload.FieldDescriptor;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.payload.RequestFieldsSnippet;
import org.springframework.restdocs.payload.ResponseFieldsSnippet;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.request.RequestParametersSnippet;
import org.springframework.restdocs.snippet.Attributes;
import org.springframework.util.StringUtils;

/**
 * Helper class for getting field descriptors for various DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
final class Snippets {

    static final ResponseHeadersSnippet LOCATION_HEADER = HeaderDocumentation.responseHeaders(
        HeaderDocumentation.headerWithName(HttpHeaders.LOCATION).description("The URI")
    );

    static final ResponseHeadersSnippet HAL_CONTENT_TYPE_HEADER = HeaderDocumentation.responseHeaders(
        HeaderDocumentation.headerWithName(HttpHeaders.CONTENT_TYPE).description(MediaTypes.HAL_JSON_VALUE)
    );

//    static final ResponseFieldsSnippet ERROR_FIELDS = PayloadDocumentation.responseFields(
//        PayloadDocumentation.fieldWithPath("error").description("The HTTP error that occurred, e.g. `Bad Request`"),
//        PayloadDocumentation.fieldWithPath("message").description("A description of the cause of the error"),
//        PayloadDocumentation.fieldWithPath("path").description("The path to which the request was made"),
//        PayloadDocumentation.fieldWithPath("status").description("The HTTP status code, e.g. `400`"),
//        PayloadDocumentation.fieldWithPath("timestamp")
//            .description("The time, in milliseconds, at which the error occurred")
//    );

    static final LinksSnippet SEARCH_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation.linkWithRel("self").description("The current search"),
        HypermediaDocumentation.linkWithRel("first").description("The first page for this search").optional(),
        HypermediaDocumentation.linkWithRel("prev").description("The previous page for this search").optional(),
        HypermediaDocumentation.linkWithRel("next").description("The next page for this search").optional(),
        HypermediaDocumentation.linkWithRel("last").description("The last page for this search").optional()
    );

    static final RequestParametersSnippet APPLICATION_SEARCH_QUERY_PARAMETERS = RequestDocumentation.requestParameters(
        RequestDocumentation.parameterWithName("name").description("The name of the applications to find.").optional(),
        RequestDocumentation.parameterWithName("user").description("The user of the applications to find.").optional(),
        RequestDocumentation.parameterWithName("status").description("The status of the applications to find.")
            .optional(),
        RequestDocumentation.parameterWithName("tag").description("The tag(s) of the applications to find.").optional(),
        RequestDocumentation.parameterWithName("type").description("The type of the applications to find.").optional(),
        RequestDocumentation.parameterWithName("page").description("The page number to get. Default to 0.").optional(),
        RequestDocumentation.parameterWithName("size").description("The size of the page to get. Default to 64.")
            .optional(),
        RequestDocumentation.parameterWithName("sort")
            .description("The fields to sort the results by. Defaults to 'updated,desc'.")
            .optional()
    );
    static final ResponseFieldsSnippet APPLICATION_SEARCH_RESULT_FIELDS = PayloadDocumentation.responseFields(
        PayloadDocumentation.fieldWithPath("_embedded.applicationList").description("The found applications."),
        PayloadDocumentation.fieldWithPath("_links").description("<<resources-index-links,Links>> to other resources."),
        PayloadDocumentation.fieldWithPath("page").description("The result page information."),
        PayloadDocumentation.fieldWithPath("page.size").description("The number of elements in this page result."),
        PayloadDocumentation.fieldWithPath("page.totalElements")
            .description("The total number of elements this search result could return."),
        PayloadDocumentation.fieldWithPath("page.totalPages")
            .description("The total number of pages there could be at the current page size."),
        PayloadDocumentation.fieldWithPath("page.number").description("The current page number.")
    );

    static final LinksSnippet APPLICATION_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation.linkWithRel("self").description("URI for this application"),
        HypermediaDocumentation.linkWithRel("commands").description("Get all the commands using this application")
    );

    static final RequestFieldsSnippet APPLICATION_REQUEST_PAYLOAD
        = PayloadDocumentation.requestFields(getApplicationFieldDescriptors());

    static final ResponseFieldsSnippet APPLICATION_RESPONSE_PAYLOAD = PayloadDocumentation
        .responseFields(getApplicationFieldDescriptors())
        .and(
            PayloadDocumentation
                .fieldWithPath("_links").description("<<resources-index-links,Links>> to other resources")
        );

    private Snippets() {
    }

    private static FieldDescriptor[] getApplicationFieldDescriptors() {
        final ConstraintDescriptions constraintDescriptions = new ConstraintDescriptions(Application.class);
        return new FieldDescriptor[]
            {
                PayloadDocumentation
                    .fieldWithPath("id")
                    .attributes(getConstraintsForField(constraintDescriptions, "id"))
                    .description("The id of the application. If not set the system will set one.")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("created")
                    .attributes(getConstraintsForField(constraintDescriptions, "created"))
                    .description("The time the application was last created. Set by system. ISO8601 with millis.")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("updated")
                    .attributes(getConstraintsForField(constraintDescriptions, "updated"))
                    .description("The time the application was last updated. Set by system. ISO8601 with millis.")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("name")
                    .attributes(getConstraintsForField(constraintDescriptions, "name"))
                    .description("The name of the application"),
                PayloadDocumentation
                    .fieldWithPath("user")
                    .attributes(getConstraintsForField(constraintDescriptions, "user"))
                    .description("The user who created the application"),
                PayloadDocumentation
                    .fieldWithPath("version")
                    .attributes(getConstraintsForField(constraintDescriptions, "version"))
                    .description("The version of the application"),
                PayloadDocumentation
                    .fieldWithPath("description")
                    .attributes(getConstraintsForField(constraintDescriptions, "description"))
                    .description("Any description for the application.")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("type")
                    .attributes(getConstraintsForField(constraintDescriptions, "type"))
                    .description("The type of application this is (e.g. hadoop, presto, spark). Can be used to group.")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("status")
                    .attributes(getConstraintsForField(constraintDescriptions, "status"))
                    .description("The status of the application"),
                PayloadDocumentation
                    .fieldWithPath("tags")
                    .attributes(getConstraintsForField(constraintDescriptions, "tags"))
                    .description("The tags for the application")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("dependencies")
                    .attributes(getConstraintsForField(constraintDescriptions, "dependencies"))
                    .description("The dependencies for the application")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("setupFile")
                    .attributes(getConstraintsForField(constraintDescriptions, "setupFile"))
                    .description("A location for any setup that needs to be done when installing")
                    .optional(),
                PayloadDocumentation
                    .fieldWithPath("configs")
                    .attributes(getConstraintsForField(constraintDescriptions, "configs"))
                    .description("Any configuration files needed for the application")
                    .optional(),
            };
    }

    private static Attributes.Attribute getConstraintsForField(
        final ConstraintDescriptions constraints,
        final String fieldName
    ) {
        return Attributes
            .key("constraints")
            .value(StringUtils.collectionToDelimitedString(constraints.descriptionsForProperty(fieldName), ". "));
    }
}
