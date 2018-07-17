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
package com.netflix.genie.client.apis;

import com.netflix.genie.client.security.oauth2.AccessToken;
import retrofit2.Call;
import retrofit2.http.FieldMap;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.POST;
import retrofit2.http.Url;

import java.util.Map;

/**
 * A interface to fetch access tokens.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface TokenService {

    /**
     * A method to retrive oauth tokens from the server.
     *
     * @param params A map of all the fields needed to fetch credentials.
     * @param url    The URL of the IDP from where to get the credentials.
     * @return A callable object.
     */
    @FormUrlEncoded
    @POST
    Call<AccessToken> getToken(@Url String url, @FieldMap Map<String, String> params);
}
