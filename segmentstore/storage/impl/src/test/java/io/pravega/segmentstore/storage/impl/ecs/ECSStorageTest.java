/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.ecs;

import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CopyObjectResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Unit tests for ECSStorage.
 */
@Slf4j
public class ECSStorageTest extends StorageTestBase {
    private ECSStorageConfig adapterConfig;
    private S3JerseyClient client = null;
    private S3Proxy s3Proxy;

    @Before
    public void setUp() throws Exception {
        S3Config ecsConfig = null;

        String endpoint = "http://127.0.0.1:9020";
        URI uri = URI.create(endpoint);
        Properties properties = new Properties();
        properties.setProperty("s3proxy.authorization", "none");
        properties.setProperty("s3proxy.endpoint", endpoint);
        properties.setProperty("jclouds.provider", "filesystem");
        properties.setProperty("jclouds.filesystem.basedir", "/tmp/s3proxy");

        ContextBuilder builder = ContextBuilder
                .newBuilder("filesystem")
                .credentials("x", "x")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(properties);
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();

        s3Proxy = S3Proxy.builder().awsAuthentication( AuthenticationType.AWS_V2_OR_V4, "x", "x")
                .endpoint(uri)
                .keyStore("", "")
                .blobStore(blobStore)
                .ignoreUnknownHeaders(true)
                .build();

        s3Proxy.start();

        this.adapterConfig = ECSStorageConfig.builder()
                .with(ECSStorageConfig.ECS_BUCKET, "kanpravegatest")
                .with(ECSStorageConfig.ECS_ACCESS_KEY_ID, "x")
                .with(ECSStorageConfig.ECS_SECRET_KEY, "x")
                .with(ECSStorageConfig.ROOT, "test")
                .build();
        if (client == null) {
            try {
                createStorage();

                try {
                    client.createBucket("kanpravegatest");
                } catch (Exception e) {
                    if (!( e instanceof S3Exception && ((S3Exception) e).getErrorCode().
                            equals("BucketAlreadyOwnedByYou"))) {
                        throw e;
                    }
                }
                List<ObjectKey> keys = client.listObjects("kanpravegatest").getObjects().stream().map((object) -> {
                    return new ObjectKey(object.getKey());
                }).collect(Collectors.toList());

                client.deleteObjects(new DeleteObjectsRequest("kanpravegatest").withKeys(keys));
            } catch (Exception e) {
                log.error("Wrong ECS URI {}. Can not continue.");
            }
        }
    }

    @After
    public void tearDown() {
        try {
            client.shutdown();
            client = null;
            s3Proxy.stop();
        } catch (Exception e) {
        }
    }

    //region Fencing tests

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * Part 1: Creation:
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * * We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Ignore
    @Override
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

    private void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        AssertExtensions.assertThrows(
                "Write was not fenced out.",
                () -> storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Concat was not fenced out.",
                () -> storage.concat(handle, si.getLength(), concatHandle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatHandle, TIMEOUT).join();
    }

    private void verifyFinalWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        storage.seal(handle, TIMEOUT).join();
        storage.delete(handle, TIMEOUT).join();

        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    private void verifyFinalWriteOperationsFail(SegmentHandle handle, Storage storage) {
        AssertExtensions.assertThrows(
                "Seal was allowed on fenced Storage.",
                () -> storage.seal(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment was sealed after rejected call to seal.", si.isSealed());

        AssertExtensions.assertThrows(
                "Delete was allowed on fenced Storage.",
                () -> storage.delete(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment was deleted after rejected call to delete.", exists);
    }

    /**
     * Tests the write() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
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

            /* Check bad offset.
            final long finalOffset = offset;
            assertThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);
            */
            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    //endregion

    @Override
    protected Storage createStorage() {
        S3Config ecsConfig = null;
        try {
            ecsConfig = new S3Config(new URI("http://localhost:9020"));
            if (adapterConfig == null) {
                setUp();
            }
            ecsConfig.withIdentity(adapterConfig.getEcsAccessKey()).withSecretKey(adapterConfig.getEcsSecretKey());

            client = new S3JerseyClientWrapper(ecsConfig);
            return new ECSStorage(client, this.adapterConfig, executorService());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return ECSSegmentHandle.getReadHandle(segmentName);
        } else {
            return ECSSegmentHandle.getWriteHandle(segmentName);
        }
    }


    private static class S3JerseyClientWrapper extends S3JerseyClient {
        private static final ConcurrentMap<String, AccessControlList> ACL_MAP = new ConcurrentHashMap<>();

        public S3JerseyClientWrapper(S3Config ecsConfig) {
            super(ecsConfig);
        }

        @Override
        public PutObjectResult putObject(PutObjectRequest request) {
            S3ObjectMetadata metadata = request.getObjectMetadata();

           if ( request.getObjectMetadata() != null ) {
            request.setObjectMetadata(null);
           }
            PutObjectResult retVal = super.putObject(request);
           if (request.getAcl() != null) {
               ACL_MAP.put(request.getKey(), request.getAcl());
           }
           return retVal;
        }

        @Override
        public void putObject(String bucketName, String key, Range range, Object content) {
            byte[] totalByes = new byte[Math.toIntExact(range.getLast() + 1)];
            try {
                if ( range.getFirst() != 0 ) {
                    int bytesRead = getObject(bucketName, key).getObject().read(totalByes, 0,
                            Math.toIntExact(range.getFirst()));
                    if ( bytesRead != range.getFirst() ) {
                        throw new IllegalArgumentException();
                    }
                }
                int bytesRead = ( (InputStream) content).read(totalByes, Math.toIntExact(range.getFirst()),
                        Math.toIntExact(range.getLast() + 1 - range.getFirst()));

                if ( bytesRead != range.getLast() + 1 - range.getFirst()) {
                    throw new IllegalArgumentException();
                }
                super.putObject( new PutObjectRequest(bucketName, key, (Object) new ByteArrayInputStream(totalByes)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
            AccessControlList retVal = ACL_MAP.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", key);
            }
            ACL_MAP.put(key, acl);
        }

        @Override
        public void setObjectAcl(SetObjectAclRequest request) {
            AccessControlList retVal = ACL_MAP.get(request.getKey());
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
            }
            ACL_MAP.put(request.getKey(), request.getAcl());
        }

        @Override
        public AccessControlList getObjectAcl(String bucketName, String key) {
            AccessControlList retVal = ACL_MAP.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", key);
            }
            return retVal;
        }

        public CopyPartResult copyPart(CopyPartRequest request) {
            if ( ACL_MAP.get(request.getKey()) == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
            }

            CopyObjectResult result = executeRequest(client, request, CopyObjectResult.class);
            CopyPartResult retVal = new CopyPartResult();
            retVal.setPartNumber(request.getPartNumber());
            retVal.setETag(result.getETag());
            return retVal;
        }

        @Override
        public void deleteObject(String bucketName, String key) {
            super.deleteObject(bucketName, key);
            ACL_MAP.remove(key);
        }
    }
}
