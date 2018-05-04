package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.utils.locks.CloseableLock;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import com.netflix.genie.test.categories.UnitTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests for fetching cache service implementation.
 */
@Slf4j
@Category(UnitTest.class)
public class FetchingCacheServiceImplTests {

    /**
     * Temporary folder.
     */
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private URI uri;

    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    //Boolean flag representing that a downloadCompleted
    private AtomicBoolean downloadCompleted = new AtomicBoolean(false);

    //CloseableLock used for the downloadComplete condition while simulating a download
    private ReentrantLock simulateDownloadLock = new ReentrantLock();

    //Condition used during download simulation for waiting
    private Condition downloadComplete = simulateDownloadLock.newCondition();

    private ArgumentDelegates.CacheArguments cacheArguments;

    private File targetFile;

    private ThreadPoolTaskExecutor cleanUpTaskExecutor;

    /**
     * Set up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        temporaryFolder.create();
        uri = new URI("https://my-server.com/path/to/config/config.xml");
        cacheArguments = Mockito.mock(ArgumentDelegates.CacheArguments.class);
        Mockito.when(
            cacheArguments.getCacheDirectory()
        ).thenReturn(temporaryFolder.getRoot());
        targetFile = new File(temporaryFolder.getRoot(), "target");
        cleanUpTaskExecutor = new ThreadPoolTaskExecutor();
        cleanUpTaskExecutor.setCorePoolSize(1);
        cleanUpTaskExecutor.initialize();
    }

    /**
     * Start two concurrent resource fetches.
     * Make sure only one cache does the download/enters the critical section
     *
     * @throws Exception the exception
     */
    @Test
    public void cacheConcurrentFetches() throws Exception {
        final AtomicInteger numOfCacheMisses = new AtomicInteger(0);

        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final Resource resource = Mockito.mock(Resource.class);

        final ReentrantLock lockBackingMock = new ReentrantLock();

        //Latch to represent that lock acquisition was attempted
        final CountDownLatch lockAquisitionAttempted = new CountDownLatch(2);

        //A mock lock backed by a reentrant lock guarding the resource
        final CloseableLock resourceLock = Mockito.mock(CloseableLock.class);
        Mockito.doAnswer(invocation -> {
            lockAquisitionAttempted.countDown();
            lockBackingMock.lock();
            return null;
        }).when(resourceLock).lock();

        Mockito.doAnswer(invocation -> {
            lockBackingMock.unlock();
            return null;
        }).when(resourceLock).close();

        //Mock locking factory to use the reentrant lock backed lock
        final FileLockFactory fileLockFactory = Mockito.mock(FileLockFactory.class);

        Mockito.when(
            fileLockFactory.getLock(Mockito.any())
        ).thenReturn(resourceLock);

        final ResourceLoader resourceLoader2 = Mockito.mock(ResourceLoader.class);
        final Resource resource2 = Mockito.mock(Resource.class);

        //Set up first cache
        Mockito.when(
            resourceLoader.getResource(Mockito.anyString())
        ).thenReturn(resource);

        Mockito.when(
            resource.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                numOfCacheMisses.incrementAndGet();
                return simulateDownloadWithWait();
            }
        );

        Mockito.when(resource.exists()).thenReturn(true);
        final FetchingCacheServiceImpl cache1 = new FetchingCacheServiceImpl(
            resourceLoader,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        //Set up the second cache
        Mockito.when(
            resourceLoader2.getResource(Mockito.anyString())
        ).thenReturn(resource2);

        Mockito.when(
            resource2.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                numOfCacheMisses.incrementAndGet();
                return simulateDownloadWithWait();
            }
        );

        Mockito.when(resource2.exists()).thenReturn(true);

        final FetchingCacheServiceImpl cache2 = new FetchingCacheServiceImpl(
            resourceLoader2,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        //Before submitting make sure conditions are set correctly
        downloadCompleted.set(false);
        numOfCacheMisses.set(0);
        final CountDownLatch allFetchesDone = new CountDownLatch(2);

        //Get resource via first cache
        executorService.submit(() -> {
            try {
                cache1.get(uri, targetFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        executorService.submit(() -> {
            try {
                cache2.get(uri, targetFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        //Wait for both threads to try to lock
        lockAquisitionAttempted.await();


        //Either one thread would have tried a download or would try shortly.
        //So, either one thread is waiting on a download or will try to download.
        //Regardless since both threads have atleast tried to lock once,
        //signal download completed
        downloadCompleted.set(true);
        simulateDownloadLock.lock();
        try {
            downloadComplete.signal();
        } finally {
            simulateDownloadLock.unlock();
        }

        //Wait for all threads to be done before asserting
        allFetchesDone.await();

        //Only one thread reached the critical section
        Assert.assertTrue(numOfCacheMisses.get() == 1);
    }

    /**
     * Start two concurrent resource fetches.
     * Make the first one to start download fail
     * Make sure a cache files exist because the other fetch succeeds
     *
     * @throws Exception the exception
     */
    @Test
    public void cacheConcurrentFetchFailOne() throws Exception {
        final AtomicInteger numOfCacheMisses = new AtomicInteger(0);
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final Resource resource = Mockito.mock(Resource.class);

        final CountDownLatch lockAquisitionAttempted = new CountDownLatch(2);

        final ReentrantLock lockBackingMock = new ReentrantLock();
        //A mock lock backed by a reentant lock guarding the resource
        final CloseableLock resourceLock = Mockito.mock(CloseableLock.class);
        Mockito.doAnswer(invocation -> {
            lockAquisitionAttempted.countDown();
            lockBackingMock.lock();
            return null;
        }).when(resourceLock).lock();

        Mockito.doAnswer(invocation -> {
            lockBackingMock.unlock();
            return null;
        }).when(resourceLock).close();

        //Mock locking facrory to use the reentrant lock backed lock
        final FileLockFactory fileLockFactory = Mockito.mock(FileLockFactory.class);

        Mockito.when(
            fileLockFactory.getLock(Mockito.any())
        ).thenReturn(resourceLock);

        final ResourceLoader resourceLoader2 = Mockito.mock(ResourceLoader.class);
        final Resource resource2 = Mockito.mock(Resource.class);

        //Set up first cache
        Mockito.when(
            resourceLoader.getResource(Mockito.anyString())
        ).thenReturn(resource);

        Mockito.when(
            resource.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                //first thread trying to download
                if (numOfCacheMisses.incrementAndGet() == 1) {
                    return simulateDownloadFailureWithWait();
                } else { // second thread doing the download
                    return Mockito.spy(
                        new ByteArrayInputStream("".getBytes(Charset.forName("UTF-8")))
                    );
                }
            }
        );

        final long lastModifiedTimeStamp = System.currentTimeMillis();
        Mockito.when(resource.exists()).thenReturn(true);
        Mockito.when(resource.lastModified()).thenReturn(lastModifiedTimeStamp);

        final FetchingCacheServiceImpl cache1 = new FetchingCacheServiceImpl(
            resourceLoader,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        //Set up the second cache
        Mockito.when(
            resourceLoader2.getResource(Mockito.anyString())
        ).thenReturn(resource2);

        Mockito.when(
            resource2.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                //first thread trying to download
                if (numOfCacheMisses.incrementAndGet() == 1) {
                    return simulateDownloadFailureWithWait();
                } else { // second thread doing the download
                    return Mockito.spy(
                        new ByteArrayInputStream("".getBytes(Charset.forName("UTF-8")))
                    );
                }
            }
        );

        Mockito.when(resource2.exists()).thenReturn(true);
        Mockito.when(resource2.lastModified()).thenReturn(lastModifiedTimeStamp);

        final FetchingCacheServiceImpl cache2 = new FetchingCacheServiceImpl(
            resourceLoader2,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        //Before submitting make sure conditions are set correctly
        downloadCompleted.set(false);
        numOfCacheMisses.set(0);
        final CountDownLatch allFetchesDone = new CountDownLatch(2);

        //Get resource via first cache
        executorService.submit(() -> {
            try {
                cache1.get(uri, targetFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        executorService.submit(() -> {
            try {
                cache2.get(uri, targetFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        //Wait for both threads to try to lock
        lockAquisitionAttempted.await();

        //Either one thread would have tried a download or would try shortly.
        //So, either one thread is waiting on a download or will try to download.
        //Regardless since both threads have atleast tried to lock once,
        //signal download completed
        downloadCompleted.set(true);
        simulateDownloadLock.lock();
        try {
            downloadComplete.signal();
        } finally {
            simulateDownloadLock.unlock();
        }

        //Wait for all threads to be done before asserting
        allFetchesDone.await();

        //Both threads reached the critical section
        Assert.assertTrue(numOfCacheMisses.get() == 2);
        //Proper directory structure and files exist for the resource
        Assert.assertTrue(
            directoryStructureExists(
                cache1.getResourceCacheId(uri),
                lastModifiedTimeStamp,
                cache1
            )
        );
    }

    /**
     * Make one cache download a resource version.
     * While first one is downloading, make another thread
     * delete the same version
     * In the end, the previous resource should not exist
     *
     * @throws Exception the exception
     */
    @Test
    public void fetchAndDeleteResourceConcurrently() throws Exception {

        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final Resource resource = Mockito.mock(Resource.class);

        final CountDownLatch lockAquisitionAttempted = new CountDownLatch(2);

        final ReentrantLock lockBackingMock = new ReentrantLock();

        //A mock lock backed by a reentant lock guarding the resource
        final CloseableLock resourceLock = Mockito.mock(CloseableLock.class);
        Mockito.doAnswer(invocation -> {
            lockAquisitionAttempted.countDown();
            lockBackingMock.lock();
            return null;
        }).when(resourceLock).lock();

        Mockito.doAnswer(invocation -> {
            lockBackingMock.unlock();
            return null;
        }).when(resourceLock).close();

        //Mock locking factory to use the reentrant lock backed lock
        final FileLockFactory fileLockFactory = Mockito.mock(FileLockFactory.class);

        Mockito.when(
            fileLockFactory.getLock(Mockito.any())
        ).thenReturn(resourceLock);

        //Latch to represent that download was started
        final CountDownLatch downloadBegin = new CountDownLatch(1);

        //Set up first cache
        Mockito.when(
            resourceLoader.getResource(Mockito.anyString())
        ).thenReturn(resource);

        Mockito.when(
            resource.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                downloadBegin.countDown();
                simulateDownloadWithWait();
                return Mockito.spy(
                    new ByteArrayInputStream("".getBytes(Charset.forName("UTF-8")))
                );
            }
        );

        final long lastModifiedTimeStamp = System.currentTimeMillis();
        Mockito.when(resource.exists()).thenReturn(true);
        Mockito.when(resource.lastModified()).thenReturn(lastModifiedTimeStamp);

        final FetchingCacheServiceImpl cache1 = new FetchingCacheServiceImpl(
            resourceLoader,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        final String resourceCacheId = cache1.getResourceCacheId(uri);

        //Set the second cache
        final ResourceLoader resourceLoader2 = Mockito.mock(ResourceLoader.class);
        final FetchingCacheServiceImpl cache2 = new FetchingCacheServiceImpl(
            resourceLoader2,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        //Before submitting make sure conditions are set correctly
        final CountDownLatch allFetchesDone = new CountDownLatch(2);

        //Get resource via first cache
        executorService.submit(() -> {
            try {
                cache1.get(uri, targetFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        //Download was started, first thread in critical section
        downloadBegin.await();

        //Start deletetion.
        executorService.submit(() -> {
            try {
                //Pass in lastDownloadedVersion = lastModifiedTimeStamp + 1 to trigger
                //the deletion of the resource version = lastModifiedTimeStamp
                cache2.cleanUpOlderResourceVersions(
                    resourceCacheId,
                    lastModifiedTimeStamp + 1
                );
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        lockAquisitionAttempted.await();

        downloadCompleted.set(true);
        simulateDownloadLock.lock();
        try {
            downloadComplete.signal();
        } finally {
            simulateDownloadLock.unlock();
        }

        //Wait for all threads to be done before asserting
        allFetchesDone.await();

        //Make sure the resource is deleted
        assertResourceDeleted(cache1, resourceCacheId, lastModifiedTimeStamp);
    }

    /**
     * Make one cache delete a resource version.
     * While first one is deleting, make another thread
     * download the same version
     * In the end, the resource should exist
     *
     * @throws Exception the exception
     */
    @Test
    public void deleteAndFetchResourceConcurrently() throws Exception {

        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        final Resource resource = Mockito.mock(Resource.class);

        final AtomicInteger numJobsInLockMethod = new AtomicInteger(0);

        final ReentrantLock lockBackingMock = new ReentrantLock();

        final CountDownLatch downLoadJobEnteredLockMethod = new CountDownLatch(1);

        final CountDownLatch deletionDone = new CountDownLatch(1);

        final CountDownLatch deletionJObEnteredLockMethod = new CountDownLatch(1);

        final CountDownLatch deletionSuccessVerified = new CountDownLatch(1);

        //A mock lock backed by a reentant lock guarding the resource
        final CloseableLock resourceLock = Mockito.mock(CloseableLock.class);
        Mockito.doAnswer(invocation -> {

            //deletion thread since its submitted first as the only thread
            if (numJobsInLockMethod.incrementAndGet() == 1) {
                //make deletion wait until download thread enters the lock method
                deletionJObEnteredLockMethod.countDown();
                downLoadJobEnteredLockMethod.await();
            } else { //download thread
                downLoadJobEnteredLockMethod.countDown();
                //wait until main thread verifies that deletion was successful
                deletionSuccessVerified.await();
            }

            lockBackingMock.lock();
            return null;
        }).when(resourceLock).lock();

        Mockito.doAnswer(invocation -> {
            lockBackingMock.unlock();
            return null;
        }).when(resourceLock).close();

        //Mock locking factory to use the reentrant lock backed lock
        final FileLockFactory fileLockFactory = Mockito.mock(FileLockFactory.class);

        Mockito.when(
            fileLockFactory.getLock(Mockito.any())
        ).thenReturn(resourceLock);

        //Set up first cache
        Mockito.when(
            resourceLoader.getResource(Mockito.anyString())
        ).thenReturn(resource);

        Mockito.when(
            resource.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                return Mockito.spy(
                    new ByteArrayInputStream("".getBytes(Charset.forName("UTF-8")))
                );
            }
        );

        final long lastModifiedTimeStamp = System.currentTimeMillis();
        Mockito.when(resource.exists()).thenReturn(true);
        Mockito.when(resource.lastModified()).thenReturn(lastModifiedTimeStamp);

        final FetchingCacheServiceImpl cache1 = new FetchingCacheServiceImpl(
            resourceLoader,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        final String resourceCacheId = cache1.getResourceCacheId(uri);

        //Set the second cache
        final ResourceLoader resourceLoader2 = Mockito.mock(ResourceLoader.class);
        final FetchingCacheServiceImpl cache2 = new FetchingCacheServiceImpl(
            resourceLoader2,
            cacheArguments,
            fileLockFactory,
            cleanUpTaskExecutor
        );

        //Download the resource which needs to be deleted, else deletion will be a no op
        //Set up a separate cache for it.

        final ResourceLoader resourceLoader3 = Mockito.mock(ResourceLoader.class);
        final Resource resource3 = Mockito.mock(Resource.class);

        Mockito.when(resource3.exists()).thenReturn(true);
        Mockito.when(resource3.lastModified()).thenReturn(lastModifiedTimeStamp);
        Mockito.when(
            resource3.getInputStream()
        ).thenAnswer(
            (Answer<InputStream>) invocation -> {
                return Mockito.spy(
                    new ByteArrayInputStream("".getBytes(Charset.forName("UTF-8")))
                );
            }
        );

        Mockito.when(
            resourceLoader3.getResource(Mockito.anyString())
        ).thenReturn(resource3);

        final FetchingCacheServiceImpl cache3 = new FetchingCacheServiceImpl(
            resourceLoader3,
            cacheArguments,
            new FileLockFactory(),
            cleanUpTaskExecutor
        );

        cache3.get(uri, targetFile);

        //Assert that the cache download was successful
        assertResourceDownloaded(cache3, resourceCacheId, lastModifiedTimeStamp);

        //Before submitting the deletion and download jobs make sure conditions are set correctly
        final CountDownLatch allFetchesDone = new CountDownLatch(2);

        //Start deletion
        executorService.submit(() -> {
            try {
                //Pass in lastDownloadedVersion = lastModifiedTimeStamp + 1 to trigger
                //the deletion of the resource version = lastModifiedTimeStamp
                cache2.cleanUpOlderResourceVersions(
                    resourceCacheId,
                    lastModifiedTimeStamp + 1
                );
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                deletionDone.countDown();
                allFetchesDone.countDown();
            }
        });

        //Deletion thread in lock method
        deletionJObEnteredLockMethod.await();

        //Start download of the version being deleted.
        //This one will make the deletion thread move ahead in lock method
        //once it reaches lock method
        executorService.submit(() -> {
            try {
                cache1.get(uri, targetFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                allFetchesDone.countDown();
            }
        });

        //Wait for deletion job to be done
        deletionDone.await();

        //Assert that deletion was successful
        assertResourceDeleted(cache1, resourceCacheId, lastModifiedTimeStamp);

        //Make the download thread stop waiting in lock method
        deletionSuccessVerified.countDown();

        //Wait for all threads to be done before asserting
        allFetchesDone.await();

        //Make sure the resource downloaded is present
        assertResourceDownloaded(cache2, resourceCacheId, lastModifiedTimeStamp);
    }

    private void assertResourceDeleted(
        final FetchingCacheServiceImpl cacheService,
        final String resourceCacheId,
        final long lastModifiedTimeStamp
    ) {
        Assert.assertFalse(
            cacheService.getCacheResourceVersionDataFile(resourceCacheId, lastModifiedTimeStamp).exists()
        );
        Assert.assertFalse(
            cacheService.getCacheResourceVersionDownloadFile(resourceCacheId, lastModifiedTimeStamp).exists()
        );
        Assert.assertTrue(
            cacheService.getCacheResourceVersionLockFile(resourceCacheId, lastModifiedTimeStamp).exists()
        );
    }


    private void assertResourceDownloaded(
        final FetchingCacheServiceImpl cacheService,
        final String resourceCacheId,
        final long lastModifiedTimeStamp
    ) {
        Assert.assertTrue(
            cacheService.getCacheResourceVersionDataFile(resourceCacheId, lastModifiedTimeStamp).exists()
        );
        Assert.assertFalse(
            cacheService.getCacheResourceVersionDownloadFile(resourceCacheId, lastModifiedTimeStamp).exists()
        );
        Assert.assertTrue(
            cacheService.getCacheResourceVersionLockFile(resourceCacheId, lastModifiedTimeStamp).exists()
        );
    }

    /**
     * Simulate a download while waiting until notified.
     *
     * @return inputstream from download
     */
    public InputStream simulateDownloadWithWait() {
        simulateDownloadLock.lock();
        try {
            while (!downloadCompleted.get()) {
                downloadComplete.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            simulateDownloadLock.unlock();
        }
        return Mockito.spy(
            new ByteArrayInputStream("".getBytes(Charset.forName("UTF-8")))
        );

    }

    /**
     * Simulate a download failure while waiting until notified.
     *
     * @return inputstream from download
     * @throws DownloadException simulated error
     */
    public InputStream simulateDownloadFailureWithWait() throws DownloadException {
        simulateDownloadWithWait();
        throw new DownloadException("Simulated error downloading resource");
    }

    /**
     * Verify that all the files exist after a successful download.
     *
     * @param resourceCacheId       resource id for the cache
     * @param lastModifiedTimeStamp resource version
     * @param cacheService          cache service instance
     * @return
     */
    private boolean directoryStructureExists(
        final String resourceCacheId,
        final long lastModifiedTimeStamp,
        final FetchingCacheServiceImpl cacheService
    ) {
        return cacheService.getCacheResourceVersionDataFile(resourceCacheId, lastModifiedTimeStamp).exists()
            && cacheService.getCacheResourceVersionLockFile(resourceCacheId, lastModifiedTimeStamp).exists()
            && !cacheService.getCacheResourceVersionDownloadFile(resourceCacheId, lastModifiedTimeStamp).exists();
    }

    /**
     * Clean up.
     *
     * @throws Exception the exception
     */
    @After
    public void cleanUp() {
        cleanUpTaskExecutor.shutdown();
    }
}
