package com.netflix.genie.core.util;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Class containing unit tests for Utils.java.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class UtilsUnitTests {

    /**
     * Tests the getFileName method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test
    public void testGetFileNameFromPath() throws GenieException {
        final String testFilePath = "oo/bar/name";
        Assert.assertEquals("name", Utils.getFileNameFromPath(testFilePath));
    }
}
