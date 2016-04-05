package com.netflix.genie.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/**
 * Base class for the clients for Genie Services.
 *
 * @author amsharma
 */
public abstract class BaseGenieClient {

    //protected RestTemplate restTemplate;
    //protected String serviceBaseURL;
    protected GenieService genieService;

    protected BaseGenieClient() {

    }

    /**
     * Constructor.
     *
     * @param serviceBaseURL The URL of the GenieService.
     */
    public BaseGenieClient(
        final String serviceBaseURL
    ) {
        // Intereceptor to add the security token to each request
//        final OkHttpClient okClient = new OkHttpClient();
//        okClient.interceptors().add(new Interceptor() {
//            @Override
//            public Response intercept(
//                final Chain chain
//            ) throws IOException {
//                final Response response = chain.proceed(chain.request());
//                return response;
//            }
//        });

        final ObjectMapper mapper = new ObjectMapper().
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        final Retrofit retrofit = new Retrofit.Builder()
            .baseUrl(serviceBaseURL)
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .build();

        genieService = retrofit.create(GenieService.class);
    }

    protected String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
