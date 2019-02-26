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

package com.netflix.genie.common.internal.util

import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.NotImplementedException
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import java.nio.file.Files

@Category(UnitTest.class)
class FileBufferFactorySpec extends Specification {
    FileBufferFactory fileBufferFactory
    Random random

    void setup() {
        this.fileBufferFactory = new FileBufferFactory()
        this.random = new Random()
    }

    void cleanup() {
    }

    def "Single int read()/write() not implemented" () {
        setup:
        FileBuffer fileBuffer = fileBufferFactory.get(1000)

        when:
        fileBuffer.getOutputStream().write(3)

        then:
        thrown(NotImplementedException)

        when:
        fileBuffer.getInputStream().read()

        then:
        thrown(NotImplementedException)

        when:
        fileBuffer.close()

        then:
        !Files.exists(fileBuffer.getTemporaryFilePath())
    }

    def "Methods not implemented and methods that throw after close" () {
        setup:
        FileBuffer fileBuffer = fileBufferFactory.get(1000)
        OutputStream outputStream = fileBuffer.getOutputStream()
        InputStream inputStream = fileBuffer.getInputStream()

        when:
        inputStream.skip(10)

        then:
        thrown(IOException)

        when:
        inputStream.reset()

        then:
        thrown(IOException)

        when:
        fileBuffer.close()

        then:
        noExceptionThrown()

        when:
        inputStream.read(new byte[10])

        then:
        thrown(IOException)

        when:
        inputStream.available()

        then:
        thrown(IOException)

        when:
        inputStream.close()

        then:
        noExceptionThrown()
        when:
        outputStream.write(new byte[10])

        then:
        thrown(IOException)

        when:
        outputStream.flush()

        then:
        thrown(IOException)

        when:
        outputStream.close()

        then:
        noExceptionThrown()

        when:
        fileBuffer.close()

        then:
        noExceptionThrown()
    }

    def "Write too much data" () {
        setup:
        FileBuffer fileBuffer = fileBufferFactory.get(400)
        OutputStream outputStream = fileBuffer.getOutputStream()

        when:
        byte[] buffer = new byte[4]
        for (int i = 0; i < 100; i++) {
            outputStream.write(buffer)
            outputStream.flush()
        }

        then:
        noExceptionThrown()

        when:
        outputStream.write(buffer)

        then:
        thrown(ArrayIndexOutOfBoundsException)
    }

    @Unroll
    def "Write, read, compare [fs:#fileSize, w:#maxReadSize, r:#maxWriteSize]"(int fileSize, int maxReadSize, int maxWriteSize) {
        setup:
        byte[] originalData = new byte[fileSize]
        byte[] dataRead = new byte[fileSize]
        this.random.nextBytes(originalData)
        FileBuffer fileBuffer = this.fileBufferFactory.get(fileSize)

        when:
        writeAll(originalData, fileBuffer, maxWriteSize, 0)
        readAll(dataRead, fileBuffer, maxReadSize)

        then:
        Arrays.equals(originalData, dataRead)

        when:
        fileBuffer.close()

        then:
        !Files.exists(fileBuffer.getTemporaryFilePath())

        where:
        fileSize | maxReadSize | maxWriteSize
        0        | 1           | 1
        300      | 1           | 1
        3000     | 10          | 100
        30000    | 100         | 1000
        30000    | 1000        | 100
    }

    @Unroll
    @Timeout(60)
    def "Write, read, compare interleaved [fs:#fileSize, r:#maxReadSize, w:#maxWriteSize, s:#maxSleep]"(int fileSize, int maxReadSize, int maxWriteSize, int maxSleep) {
        setup:
        byte[] originalData = new byte[fileSize]
        byte[] dataRead = new byte[fileSize]
        this.random.nextBytes(originalData)
        FileBuffer fileBuffer = this.fileBufferFactory.get(fileSize)
        FileBufferFactorySpec test
        test = this

        when:
        Thread reader = new Thread(
            new Runnable() {
                @Override
                void run() {
                    test.readAll(dataRead, fileBuffer, maxReadSize)
                }
            },
            "Reader")
        Thread writer = new Thread(
            new Runnable() {
                @Override
                void run() {
                    test.writeAll(originalData, fileBuffer, maxWriteSize, maxSleep)
                }
            },
            "Writer")
        reader.start()
        writer.start()
        writer.join()
        reader.join()

        then:
        Arrays.equals(originalData, dataRead)

        when:
        fileBuffer.close()

        then:
        !Files.exists(fileBuffer.getTemporaryFilePath())

        where:
        fileSize | maxReadSize | maxWriteSize | maxSleep
        0        | 1           | 1            | 300
        10       | 8           | 8            | 100
        30       | 1           | 1            | 100
        30       | 5           | 5            | 100
        3000     | 10          | 100          | 50
        3000     | 100         | 10           | 10
        30000    | 100         | 1000         | 100
        30000    | 1000        | 100          | 10
    }

    void readAll(byte[] data, FileBuffer fileBuffer, int maxReadSize) {

        InputStream inputStream = fileBuffer.getInputStream()
        byte[] buffer = new byte[maxReadSize]
        int watermark = 0

        while (true) {
            // This could be simplified by writing straight into data, without the extra copy.
            // This variant however increases coverage
            int bytesRead = inputStream.read(buffer)
            if (bytesRead == -1) {
                break
            }
            System.arraycopy(buffer, 0, data, watermark, bytesRead)
            watermark += bytesRead
            println("Read: " + bytesRead + " bytes (total: " + watermark + ")")
        }
        println("Read completed")
    }

    void writeAll(byte[] data, FileBuffer fileBuffer, int maxWrite, int maxSleep) {
        int dataSize = data.length
        for (int watermark = 0; watermark < dataSize;) {
            if (maxSleep > 0) {
                Thread.sleep(random.nextInt(maxSleep))
            }
            int dataLeft = dataSize - watermark
            int writeSize = Math.min(dataLeft, (this.random.nextInt(maxWrite) + 1))
            // This could be simplified by reading straight from data, without the extra copy.
            // This variant however increases coverage
            byte[] bytesToWrite = Arrays.copyOfRange(data, watermark, watermark + writeSize)
            fileBuffer.getOutputStream().write(bytesToWrite)
            watermark += writeSize
            println("Written " + watermark + "/" + dataSize + " bytes")
        }
        println("Write completed")
    }
}
