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

package com.netflix.genie.agent.cli

import com.beust.jcommander.IStringConverter
import com.beust.jcommander.ParameterException
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.common.internal.dto.v4.Criterion
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.categories.UnitTest
import org.assertj.core.util.Sets
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Unroll

@Category(UnitTest.class)
class ArgumentConvertersSpec extends Specification {

    @Unroll
    def "FileConverter: #pathString"(String pathString, File expectedFile) {

        expect:
        new ArgumentConverters.FileConverter().convert(pathString) == expectedFile

        where:
        pathString     | expectedFile
        "/tmp"         | new File("/tmp")
        "/tmp/"        | new File("/tmp/")
        "/tmp/foo/bar" | new File("/tmp/foo/bar")
        "/tmp/foo/"    | new File("/tmp/foo/")
    }

    @Unroll
    def "URIConverter: #uriString"(String uriString, URI expectedURI) {
        expect:
        new ArgumentConverters.URIConverter().convert(uriString) == expectedURI

        where:
        uriString                   | expectedURI
        "file:///tmp/foo"           | new URI("file:///tmp/foo")
        "s3://genie/genie/conf.xml" | new URI("s3://genie/genie/conf.xml")
        "classpath://conf.xml"      | new URI("classpath://conf.xml")
        "http://www.example.com"    | new URI("http://www.example.com")
    }

    @Unroll
    def "CriterionConverter #inputString"(String inputString, Criterion expectedCriterion) {
        setup:
        ArgumentConverters.CriterionConverter criterionConverter = new ArgumentConverters.CriterionConverter()

        expect:
        expectedCriterion == criterionConverter.convert(inputString)

        where:
        inputString                                | expectedCriterion
        "ID=123/NAME=n/STATUS=s/TAGS=tag,tag,tags" |
                new Criterion.Builder().withId("123").withName("n").withStatus("s").withTags(Sets.newHashSet(["tag", "tags"])).build()
        "NAME=n/STATUS=s"                          |
                new Criterion.Builder().withName("n").withStatus("s").build()
        "ID=123/STATUS=s"                          |
                new Criterion.Builder().withId("123").withStatus("s").build()
        "STATUS=s/TAGS=tag,tag,tags"               |
                new Criterion.Builder().withStatus("s").withTags(Sets.newHashSet(["tag", "tags"])).build()
        "ID=123/TAGS=tag,tag,tags"                 |
                new Criterion.Builder().withId("123").withTags(Sets.newHashSet(["tag", "tags"])).build()
        "ID=123"                                   |
                new Criterion.Builder().withId("123").build()
        "NAME=n"                                   |
                new Criterion.Builder().withName("n").build()
        "STATUS=s"                                 |
                new Criterion.Builder().withStatus("s").build()
        "TAGS=tag,tag,tags"                        |
                new Criterion.Builder().withTags(Sets.newHashSet(["tag", "tags"])).build()
        "ID=123/NAME=n/VERSION=v/STATUS=s/TAGS=tag,tag,tags" |
                new Criterion.Builder().withId("123").withName("n").withVersion("v").withStatus("s").withTags(Sets.newHashSet(["tag", "tags"])).build()
        "VERSION=v"                                   |
                new Criterion.Builder().withVersion("v").build()
        "NAME=n/VERSION=v"                                   |
                new Criterion.Builder().withName("n").withVersion("v").build()
        "VERSION=v/STATUS=s"                                   |
                new Criterion.Builder().withVersion("v").withStatus("s").build()
        "ID=123/NAME=n/VERSION=1.2.4/STATUS=s/TAGS=tag,tag,tags" |
                new Criterion.Builder().withId("123").withName("n").withVersion("1.2.4").withStatus("s").withTags(Sets.newHashSet(["tag", "tags"])).build()
    }

    @Unroll
    def "JSONConverter #inputString"(String inputString, JsonNode expectedJsonNode) {
        setup:
        ArgumentConverters.JSONConverter jsonConverter = new ArgumentConverters.JSONConverter()

        expect:
        expectedJsonNode == jsonConverter.convert(inputString)

        where:
        inputString                                      | expectedJsonNode
        "{}"                                             |
                GenieObjectMapper.getMapper().createObjectNode()
        "{\"strField\": \"value\", \"boolField\": true}" |
                GenieObjectMapper.getMapper().createObjectNode().put("boolField", true).put("strField", "value")
    }

    @Unroll
    def "Conversion error for #converterClass ( \"#inputString\" )"(Class<IStringConverter> converterClass, String inputString) {

        when:
        converterClass.newInstance().convert(inputString)

        then:
        thrown(ParameterException)

        where:
        converterClass                        | inputString
        ArgumentConverters.URIConverter       | "\n"
        ArgumentConverters.FileConverter      | ""
        ArgumentConverters.CriterionConverter | ""
        ArgumentConverters.CriterionConverter | "///"
        ArgumentConverters.JSONConverter      | "..."
    }
}
