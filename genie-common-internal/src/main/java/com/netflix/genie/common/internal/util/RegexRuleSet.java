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

package com.netflix.genie.common.internal.util;

import com.google.common.collect.Lists;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility class to match a string against an ordered set of regexes and obtain an accept/reject response.
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class RegexRuleSet {
    private final Response defaultResponse;
    private final List<Rule> rules;

    private RegexRuleSet(
        final List<Rule> rules,
        final Response defaultResponse
    ) {
        this.rules = Lists.newArrayList(rules);
        this.defaultResponse = defaultResponse;
    }

    /**
     * Evaluate an input string against the rule set.
     *
     * @param input an input string
     * @return a response
     */
    public Response evaluate(final String input) {
        for (final Rule rule : rules) {
            if (rule.pattern.matcher(input).matches()) {
                return rule.response;
            }
        }
        return defaultResponse;
    }

    /**
     * Evaluate an input string against the ruleset for acceptance.
     *
     * @param input an input string
     * @return true if the response for this input is ACCEPT, false otherwise
     */
    public boolean accept(final String input) {
        return evaluate(input) == Response.ACCEPT;
    }

    /**
     * Evaluate an input string against the ruleset for rejection.
     *
     * @param input an input string
     * @return true if the response for this input is REJECT, false otherwise
     */
    public boolean reject(final String input) {
        return !accept(input);
    }

    /**
     * The two responses to an input.
     */
    public enum Response {
        /**
         * Accept the input.
         */
        ACCEPT,
        /**
         * Reject the input.
         */
        REJECT,
    }

    /**
     * An individual rule in a ruleset.
     * Consists of a regular expression, and a response to return if the given input matches it.
     */
    public static final class Rule {

        private final Pattern pattern;
        private final Response response;

        private Rule(final Pattern pattern, final Response response) {
            this.pattern = pattern;
            this.response = response;
        }
    }

    /**
     * Ruleset builder.
     */
    public static class Builder {

        private final LinkedList<Rule> rules = Lists.newLinkedList();
        private final Response defaultResponse;

        /**
         * Constructor.
         *
         * @param defaultResponse the response to return if no rule is matched
         */
        public Builder(final Response defaultResponse) {
            this.defaultResponse = defaultResponse;
        }

        /**
         * Add a rule by compiling the given string into a regular expression.
         *
         * @param regexString a regex string
         * @param response    the response this rule returns if matched
         * @return the builder
         */
        public Builder addRule(final String regexString, final Response response) {
            return addRule(Pattern.compile(regexString), response);
        }

        /**
         * Add a rule by with the given pre-compiled regular expression.
         *
         * @param pattern  a pattern
         * @param response the response this rule returns if matched
         * @return the builder
         */
        public Builder addRule(final Pattern pattern, final Response response) {
            rules.add(new Rule(pattern, response));
            return this;
        }

        /**
         * Build the ruleset.
         *
         * @return a RegexRuleSet
         */
        public RegexRuleSet build() {
            return new RegexRuleSet(rules, defaultResponse);
        }
    }

    /**
     * Factory method to build a whitelist ruleset.
     * (Whitelist rejects everything except for the given patterns).
     *
     * @param patternStrings a set of pattern strings that constitute the whitelist
     * @return the ruleset
     */
    public static RegexRuleSet buildWhitelist(final String... patternStrings) {
        return buildWhitelist(compilePatternStrings(patternStrings));
    }

    /**
     * Factory method to build a whitelist ruleset.
     * (Whitelist rejects everything except for the given patterns).
     *
     * @param patterns a set of patterns that constitute the whitelist
     * @return the ruleset
     */
    public static RegexRuleSet buildWhitelist(final Pattern... patterns) {
        final Builder builder = new Builder(Response.REJECT);
        for (final Pattern pattern : patterns) {
            builder.addRule(pattern, Response.ACCEPT);
        }
        return builder.build();
    }

    /**
     * Factory method to build a whitelist ruleset.
     * (Blacklist accepts everything except for the given patterns).
     *
     * @param patternStrings a set of pattern strings that constitute the blacklist
     * @return the ruleset
     */
    public static RegexRuleSet buildBlacklist(final String... patternStrings) {
        return buildBlacklist(compilePatternStrings(patternStrings));
    }

    /**
     * Factory method to build a whitelist ruleset.
     * (Blacklist accepts everything except for the given patterns).
     *
     * @param patterns a set of patterns that constitute the blacklist
     * @return the ruleset
     */
    public static RegexRuleSet buildBlacklist(final Pattern... patterns) {
        final Builder builder = new Builder(Response.ACCEPT);
        for (final Pattern pattern : patterns) {
            builder.addRule(pattern, Response.REJECT);
        }
        return builder.build();
    }

    private static Pattern[] compilePatternStrings(final String[] patternStrings) {
        final Pattern[] patterns = new Pattern[patternStrings.length];
        for (int i = 0; i < patternStrings.length; i++) {
            patterns[i] = Pattern.compile(patternStrings[i]);
        }
        return patterns;
    }
}
