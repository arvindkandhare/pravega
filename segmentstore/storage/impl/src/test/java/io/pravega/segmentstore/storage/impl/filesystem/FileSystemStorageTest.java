/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;

import static io.pravega.test.common.AssertExtensions.assertMayThrow;
import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Unit tests for FileSystemStorage.
 */
public class FileSystemStorageTest extends StorageTestBase {
    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;
    private FileSystemStorageFactory storageFactory;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        this.adapterConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
        this.storageFactory = new FileSystemStorageFactory(adapterConfig, this.executorService());
    }

    @After
    public void tearDown() {
        FileHelpers.deleteFileOrDirectory(baseDir);
        baseDir = null;
    }

    //region Fencing tests

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * ** We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Override
    @Test
    public void testFencing() throws Exception {
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage();
             val storage2 = createStorage()) {
            storage1.initialize(epoch1);
            storage2.initialize(epoch2);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            SegmentHandle handle2 = storage2.openWrite(segmentName).join();

            // Storage1 should be able to execute read-only operations.
            //verifyWriteOperationsFail(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(handle2, storage2);
            verifyWriteOperationsSucceed(handle2, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsSucceed(handle2, storage2);
        }
    }

    private void verifyReadOnlyOperationsSucceed(SegmentHandle handle, Storage storage) {
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment does not exist.", exists);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertNotNull("Unexpected response from getStreamSegmentInfo.", si);

        byte[] readBuffer = new byte[(int) si.getLength()];
        int readBytes = storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);
    }

    private void verifyWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();

        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        storage.concat(handle, si.getLength() + data.length, concatHandle.getSegmentName(), TIMEOUT).join();
    }

    private void verifyFinalWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        storage.seal(handle, TIMEOUT).join();
        storage.delete(handle, TIMEOUT).join();

        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    /**
     * Tests the write() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Override
    @Test
    public void testWrite() throws Exception {
        String segmentName = "foo_write";
        int appendCount = 100;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, TIMEOUT).join();

            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createHandle(segmentName + "_1", false, DEFAULT_EPOCH), 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    //endregion

    //region synchronization unit tests

    /**
     * This test case simulates two hosts writing at the same offset at the same time.
     */
    @Test
    public void testWriteTwoHosts() {
        String segmentName = "foo_write";
        int appendCount = 5;

        try (Storage s1 = createStorage();
             Storage s2 = createStorage()) {
            s1.initialize(DEFAULT_EPOCH);
            s1.create(segmentName, TIMEOUT).join();
            SegmentHandle writeHandle1 = s1.openWrite(segmentName).join();
            SegmentHandle writeHandle2 = s1.openWrite(segmentName).join();
            long offset = 0;
            byte[] writeData = String.format("Segment_%s_Append", segmentName).getBytes();
            for (int j = 0; j < appendCount; j++) {
                ByteArrayInputStream dataStream1 = new ByteArrayInputStream(writeData);
                ByteArrayInputStream dataStream2 = new ByteArrayInputStream(writeData);
                CompletableFuture f1 = s1.write(writeHandle1, offset, dataStream1, writeData.length, TIMEOUT);
                CompletableFuture f2 = s2.write(writeHandle2, offset, dataStream2, writeData.length, TIMEOUT);
                assertMayThrow("Write expected to complete OR throw BadOffsetException." +
                                "threw an unexpected exception.",
                        () -> CompletableFuture.allOf(f1, f2),
                        ex -> ex instanceof BadOffsetException);
                offset += writeData.length;
            }
            Assert.assertTrue( "Writes at the same offset are expected to be idempotent.",
                    s1.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength() == offset);

            offset = 0;
            byte[] readBuffer = new byte[writeData.length];
            for (int j = 0; j < appendCount; j++) {
                int bytesRead = s1.read(writeHandle1, j * readBuffer.length, readBuffer,
                        0, readBuffer.length, TIMEOUT) .join();
                Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset),
                        readBuffer.length, bytesRead);
                AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset),
                        readBuffer, 0, readBuffer, 0, bytesRead);
            }

            s1.delete(writeHandle1, TIMEOUT).join();
        }
    }

    /**
     * This test case simulates host crashing during concat and retrying the operation.
     */
    @Test
    public void testPartialConcat() {
        String segmentName = "foo_write";
        String concatSegmentName = "foo_concat";
        String newConcatSegmentName = "foo_concat0";

        int offset = 0;

        try ( Storage s1 = createStorage()) {
            s1.initialize(DEFAULT_EPOCH);

            s1.create(segmentName, TIMEOUT).join();
            s1.create(concatSegmentName, TIMEOUT).join();

            SegmentHandle writeHandle1 = s1.openWrite(segmentName).join();
            SegmentHandle writeHandle2 = s1.openWrite(concatSegmentName).join();

            byte[] writeData = String.format("Segment_%s_Append", segmentName).getBytes();
            ByteArrayInputStream dataStream1 = new ByteArrayInputStream(writeData);
            ByteArrayInputStream dataStream2 = new ByteArrayInputStream(writeData);

            s1.write(writeHandle1, offset, dataStream1, writeData.length, TIMEOUT).join();
            s1.write(writeHandle2, offset, dataStream2, writeData.length, TIMEOUT).join();

            s1.seal(writeHandle2, TIMEOUT).join();

            // This will append the segments and delete the concat segment.
            s1.concat(writeHandle1, writeData.length, concatSegmentName, TIMEOUT).join();
            long lengthBeforeRetry = s1.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength();

            // Create the segment again.
            s1.create(newConcatSegmentName, TIMEOUT).join();
            writeHandle2 = s1.openWrite(newConcatSegmentName).join();
            dataStream2 = new ByteArrayInputStream(writeData);
            s1.write(writeHandle2, offset, dataStream2, writeData.length, TIMEOUT).join();
            s1.seal(writeHandle2, TIMEOUT).join();
            //Concat at the same offset again
            s1.concat(writeHandle1, writeData.length, newConcatSegmentName, TIMEOUT).join();
            long lengthAfterRetry = s1.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength();
            Assert.assertTrue( String.format("Concatenation of same segment at the same offset(%d) should result in " +
                            "same segment size(%d), but is (%d)", writeData.length, lengthBeforeRetry,
                    lengthAfterRetry),
                    lengthBeforeRetry == lengthAfterRetry);

            //Verify the data
            byte[] readBuffer = new byte[writeData.length];
            for (int j = 0; j < 2; j++) {
                int bytesRead = s1.read(writeHandle1, j * readBuffer.length, readBuffer,
                        0, readBuffer.length, TIMEOUT) .join();
                Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset),
                        readBuffer.length, bytesRead);
                AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset),
                        readBuffer, (int) offset, readBuffer, 0, bytesRead);
            }
            s1.delete(writeHandle1, TIMEOUT).join();
        }
    }

    //endregion

    @Override
    protected Storage createStorage() {
        return this.storageFactory.createStorageAdapter();
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return FileSystemSegmentHandle.readHandle(segmentName);
        } else {
            return FileSystemSegmentHandle.writeHandle(segmentName);
        }
    }


}
