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
package com.netflix.genie.client.interceptor;

import com.netflix.genie.client.security.AccessToken;
import com.netflix.genie.client.security.TokenFetcher;
import com.netflix.genie.common.exceptions.GenieException;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

/**
 * An interceptor that adds security headers to all outgoing requests.
 *
 * @author amsharma
 */
public class SecurityHeaderInterceptor implements Interceptor {

    private final TokenFetcher tokenFetcher;

    /**
     * Constructor.
     *
     * @param tokenFetcher An instance of the TokenFetcher class used to fetch OAuth Tokens.
     */
    public SecurityHeaderInterceptor(
        final TokenFetcher tokenFetcher
        ) {
        this.tokenFetcher = tokenFetcher;
    }

    @Override
    public Response intercept(
        final Chain chain
    ) throws IOException {
        try {
            final AccessToken accessToken = this.tokenFetcher.getToken();

            final Request newRequest = chain
                .request()
                .newBuilder()
                .addHeader("Authorization", accessToken.getTokenType() + " " + accessToken.getAccessToken())
                .build();

//            System.out.println(String.format("Sending request %s on %s%n%s",
//                newRequest.url(), chain.connection(), newRequest.headers()));

            return chain.proceed(newRequest);
        } catch (GenieException e) {
            throw new IOException("Genie client could not add Authorization Headers to outgoing request.");
        }
    }
}
