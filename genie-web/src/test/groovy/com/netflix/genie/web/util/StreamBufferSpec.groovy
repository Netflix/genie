/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.util

import com.google.protobuf.ByteString
import org.apache.commons.lang3.NotImplementedException
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.ThreadLocalRandom

class StreamBufferSpec extends Specification {
    StreamBuffer buffer
    Random random

    void setup() {
        this.random = new Random()
        this.buffer = new StreamBuffer()
    }

    def "Non-blocking read and write"() {
        setup:
        byte[] dataToWrite = new byte[30]
        byte[] dataRead = new byte[30]

        // Populate with random bytes
        this.random.nextBytes(dataToWrite)

        int lastReadSize
        InputStream inputStream = buffer.getInputStream()

        when:
        // W: 0-9
        this.buffer.write(ByteString.copyFrom(dataToWrite, 0, 10))
        // R: 0-9
        lastReadSize = inputStream.read(dataRead, 0, 30)

        then:
        lastReadSize == 10

        when:
        // W: 10-29
        this.buffer.write(ByteString.copyFrom(dataToWrite, 10, 20))
        // R: 10-19
        lastReadSize = inputStream.read(dataRead, 10, 10)

        then:
        lastReadSize == 10

        when:
        // R: nothing
        lastReadSize = inputStream.read(dataRead, 20, 0)

        then:
        lastReadSize == 0

        when:
        // R: 20-29
        lastReadSize = inputStream.read(dataRead, 20, 10)

        then:
        lastReadSize == 10

        when:
        this.buffer.closeForCompleted()
        lastReadSize = inputStream.read(dataRead, 20, 10)

        then:
        lastReadSize == -1
        dataToWrite == dataRead
    }

    def "Write after closing"() {

        when:
        this.buffer.write(ByteString.copyFromUtf8("Hello World!"))

        then:
        noExceptionThrown()

        when:
        this.buffer.closeForError(new RuntimeException("..."))

        then:
        noExceptionThrown()

        when:
        this.buffer.write(ByteString.copyFromUtf8("Hello World!"))

        then:
        thrown(IllegalStateException)
    }

    def "Get input stream twice"() {
        InputStream inputStream

        when:
        inputStream = this.buffer.getInputStream()

        then:
        inputStream != null

        when:
        this.buffer.getInputStream()

        then:
        thrown(IllegalStateException)
    }

    def "Invalid calls to input stream"() {
        byte[] b = new byte[10]
        InputStream inputStream

        when:
        inputStream = this.buffer.getInputStream()

        then:
        inputStream != null

        when:
        inputStream.read(b, -1, 10)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        inputStream.read(b, 0, -1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        inputStream.read(b, 0, 11)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        inputStream.read()

        then:
        thrown(NotImplementedException)
    }

    @Timeout(value = 10)
    def "Multi-threaded access"() {
        setup:
        int dataSize = 3000
        int maxWriteSize = 30
        float sleepLikelihood = 0.2
        int maxSleepMillis = 100
        byte[] inputData = new byte[dataSize]
        byte[] outputData = new byte[dataSize]

        // Populate with random bytes
        this.random.nextBytes(inputData)

        InputStream inputStream = this.buffer.getInputStream()

        Thread writeThread = new Thread(new Runnable() {
            @Override
            void run() {
                ThreadLocalRandom random = new ThreadLocalRandom()
                int offset = 0

                while (offset < dataSize) {
                    int dataLeft = dataSize - offset
                    int size = 1 + random.nextInt(Math.min(maxWriteSize, dataLeft))
                    println("Write: " + size)
                    buffer.write(ByteString.copyFrom(inputData, offset, size))
                    offset += size

                    if (random.nextFloat() <= sleepLikelihood) {
                        System.sleep(random.nextInt(maxSleepMillis))
                    }

                }
                buffer.closeForCompleted()
            }
        })

        Thread readThread = new Thread(new Runnable() {
            @Override
            void run() {

                ThreadLocalRandom random = new ThreadLocalRandom()

                int offset = 0
                while (true) {
                    int bytesRead = inputStream.read(outputData, offset, dataSize - offset)
                    println("Read: " + bytesRead)
                    if (bytesRead == -1) {
                        break
                    }
                    offset += bytesRead

                    if (random.nextFloat() <= sleepLikelihood) {
                        System.sleep(random.nextInt(maxSleepMillis))
                    }
                }
            }
        })

        when:
        writeThread.start()
        readThread.start()
        writeThread.join()
        readThread.join()

        then:
        inputData == outputData
    }
}
