package com.netflix.genie.common.internal.aws.s3;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "cloud.aws.s3")
public class S3ResourceLoaderProperties {
    private int corePoolSize = 1;
    private int maxPoolSize = Integer.MAX_VALUE;
    private int queueCapacity = Integer.MAX_VALUE;
}
