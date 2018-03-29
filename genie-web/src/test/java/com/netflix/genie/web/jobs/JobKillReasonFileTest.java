package com.netflix.genie.web.jobs;

import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Unit tests for the JobKillReasonFile class.
 *
 * @author mprimi
 * @since 3.0.8
 */
@Category(UnitTest.class)
public class JobKillReasonFileTest {

    private static final String KILL_REASON_STRING = "Test";

    /**
     * Test serialization and deserialization of JobKillReasonFile.
     *
     * @throws IOException in case of serialization error
     */
    @Test
    public void serializeThenLoad() throws IOException {

        final JobKillReasonFile originalJobKillReasonFile = new JobKillReasonFile(KILL_REASON_STRING);

        Assert.assertEquals(KILL_REASON_STRING, originalJobKillReasonFile.getKillReason());

        final byte[] bytes = GenieObjectMapper.getMapper().writeValueAsBytes(originalJobKillReasonFile);

        final JobKillReasonFile loadedJobKillReasonFile
            = GenieObjectMapper.getMapper().readValue(bytes, JobKillReasonFile.class);

        Assert.assertEquals(KILL_REASON_STRING, loadedJobKillReasonFile.getKillReason());
    }
}
