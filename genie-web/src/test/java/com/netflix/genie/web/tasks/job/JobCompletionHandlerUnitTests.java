package com.netflix.genie.web.tasks.job;

import com.netflix.genie.test.categories.UnitTest;
import org.junit.experimental.categories.Category;

/**
 * Unit Tests for the class JobCompletionHandler.
 *
 * @author amsharma
 * @since 3.0.0
 *
 */
@Category(UnitTest.class)
public class JobCompletionHandlerUnitTests {

//    private JobPersistenceService jobPersistenceService;
//    private JobSearchService jobSearchService;
//    private Resource genieWorkingDir;
//    private JobCompletionHandler jobCompletionHandler;
//    private JobFinishedEvent jobFinishedEvent;
//    private GenieFileTransferService genieFileTransferService;
//
//    /**
//     * Set up the classes for the tests.
//     *
//     * @throws GenieException If there is any problem.
//     */
//    @Before
//    public void setUp() throws GenieException {
//        jobPersistenceService = Mockito.mock(JobPersistenceService.class);
//        jobSearchService = Mockito.mock(JobSearchService.class);
//        genieWorkingDir = new DefaultResourceLoader().getResource("file:///mnt/tomcat");
//        jobFinishedEvent = Mockito.mock(JobFinishedEvent.class);
//        genieFileTransferService = Mockito.mock(GenieFileTransferService.class);
//
////        jobCompletionHandler = new JobCompletionHandler(
////            jobPersistenceService,
////            jobSearchService,
////            genieFileTransferService,
////            genieWorkingDir
////        );
//    }
//
////    /**
////     * Test the Archive Job Directory method.
////     *
////     * @throws GenieException If there is any problem
////     */
////    @Test
////    public void testArchiveJobDirMethod() throws GenieException {
////        this.jobCompletionHandler.archivedJobDir(this.jobFinishedEvent);
////    }
}
