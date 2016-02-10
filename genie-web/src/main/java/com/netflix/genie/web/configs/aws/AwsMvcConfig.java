package com.netflix.genie.web.configs.aws;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.IOException;

/**
 * Beans and configuration specifically for MVC on AWS.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Profile("aws")
@Configuration
@Slf4j
public class AwsMvcConfig {

    /**
     * Get the hostname for this application by calling the AWS metadata endpoints. Overrides default implementation
     * which defaults to using InetAddress class. Only active when profile enabled.
     *
     * @param httpClient The http client to use to call the Amazon endpoints
     * @return The hostname
     * @throws IOException When the host can't be calculated
     */
    @Bean
    public String hostname(final HttpClient httpClient) throws IOException {
        return "b";

//        // See: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html
//        final HttpGet publicHostnameGet = new HttpGet("http://169.254.169.254/latest/meta-data/public-hostname");
//        final HttpGet localIPV4HostnameGet = new HttpGet("http://169.254.169.254/latest/meta-data/local-ipv4");
//
//        final HttpResponse publicHostnameResponse = httpClient.execute(publicHostnameGet);
//        if (publicHostnameResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
//            final String hostname = EntityUtils.toString(publicHostnameResponse.getEntity());
//            log.info("\n\n\nPublic Hostname: {}\n\n\n", hostname);
//            return hostname;
//        }
//
//        final HttpResponse ipv4Response = httpClient.execute(localIPV4HostnameGet);
//        if (ipv4Response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
//            final String hostname = EntityUtils.toString(ipv4Response.getEntity());
//            log.info("\n\n\nIPV4 Hostname: {}\n\n\n", hostname);
//            return hostname;
//        }
//
//        throw new IOException("Unable to get instance metadata from AWS");
    }
}
