package com.netflix.genie.agent.rpc;

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.test.categories.IntegrationTest;
import io.grpc.ManagedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for GrpcConfigTest.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Category(IntegrationTest.class)
@ContextConfiguration(classes = {GrpcConfig.class, MockConfig.class})
public class GrpcConfigTest {

    @Autowired
    private ManagedChannel managedChannel1;
    @Autowired
    private ManagedChannel managedChannel2;
    @Autowired
    private PingServiceGrpc.PingServiceFutureStub pingServiceClient1;
    @Autowired
    private PingServiceGrpc.PingServiceFutureStub pingServiceClient2;

    /**
     * Check that channel is a singleton.
     *
     * @throws Exception exception
     */
    @Test
    public void channel() throws Exception {
        Assert.assertNotNull(managedChannel1);
        Assert.assertNotNull(managedChannel2);
        Assert.assertTrue(managedChannel1 == managedChannel2);
    }

    /**
     * Check that clients are not singletons.
     *
     * @throws Exception exception
     */
    @Test
    public void pingServiceClient() throws Exception {
        Assert.assertNotNull(pingServiceClient1);
        Assert.assertNotNull(pingServiceClient2);
        Assert.assertTrue(pingServiceClient1 != pingServiceClient2);
    }
}

@Configuration
class MockConfig {
    @Bean
    ArgumentDelegates.ServerArguments serverArguments() {
        final ArgumentDelegates.ServerArguments mock = Mockito.mock(ArgumentDelegates.ServerArguments.class);
        Mockito.when(mock.getServerHost()).thenReturn("server.com");
        Mockito.when(mock.getServerPort()).thenReturn(1234);
        Mockito.when(mock.getRpcTimeout()).thenReturn(3L);
        return mock;
    }
}
