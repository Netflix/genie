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

import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Class that evaluates the retrofit response code and maps it to an gitappropriate Genie Exception.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class ResponseMappingInterceptor implements Interceptor {
    /**
     * {@inheritDoc}
     */
    @Override
    public Response intercept(final Chain chain) throws IOException {
        final Response response = chain.proceed(chain.request());
        if (response.isSuccessful()) {
            return response;
        } else {
            switch (response.code()) {
                case HttpURLConnection.HTTP_PRECON_FAILED:
                    throw new IOException(new GeniePreconditionException(response.message()));
                case HttpURLConnection.HTTP_BAD_REQUEST:
                    throw new IOException(new GenieBadRequestException(response.message()));
                case HttpURLConnection.HTTP_CONFLICT:
                    throw new IOException(new GenieConflictException(response.message()));
                case HttpURLConnection.HTTP_INTERNAL_ERROR:
                    throw new IOException(new GenieServerException(response.message()));
                case HttpURLConnection.HTTP_NOT_FOUND:
                    throw new IOException(new GenieNotFoundException(response.message()));
                    // Catch all for Unmapped Errors
                default:
                    throw new IOException("Request failed. Error code: ["
                        + response.code()
                        + "], Error Message ["
                        + response.message()
                        + "]");
            }
        }
    }
}
