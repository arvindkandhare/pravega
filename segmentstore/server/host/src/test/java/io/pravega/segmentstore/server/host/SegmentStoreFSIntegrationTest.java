/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.filesystem.FileSystemStorageConfig;
import io.pravega.segmentstore.storage.impl.filesystem.FileSystemStorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.test.common.TestUtils;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class SegmentStoreFSIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 3;
    private File baseDir = null;
    private BookKeeperServiceRunner bkRunner;
    private CuratorFramework zkClient;

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        // BookKeeper
        // Pick random ports to reduce chances of collisions during concurrent test executions.
        int zkPort = TestUtils.getAvailableListenPort();
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        this.bkRunner = BookKeeperServiceRunner.builder()
                .startZk(true)
                .zkPort(zkPort)
                .ledgersPath("/ledgers")
                .bookiePorts(bookiePorts)
                .build();
        this.bkRunner.start();

        // Create a ZKClient with a base namespace.
        String baseNamespace = "pravega/" + Long.toHexString(System.nanoTime());
        this.zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + zkPort)
                .namespace(baseNamespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .build();
        this.zkClient.start();

        // Attach a sub-namespace for the Container Metadata.
        String logMetaNamespace = "segmentstore/containers";
        this.configBuilder.include(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + zkPort)
                .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers"));

        this.baseDir = Files.createTempDirectory("test_fs").toFile().getAbsoluteFile();

        this.configBuilder.include(FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath()));
    }

    /**
     * Shuts down BookKeeper and cleans up file system directory
     */
    @After
    public void tearDown() throws Exception {
        // BookKeeper
        val bk = this.bkRunner;
        if (bk != null) {
            bk.close();
            this.bkRunner = null;
        }

        val zk = this.zkClient;
        if (zk != null) {
            zk.close();
            this.zkClient = null;
        }
        FileHelpers.deleteFileOrDirectory(this.baseDir);
        this.baseDir = null;
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected synchronized ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig,
                                                        AtomicReference<Storage> storage) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    StorageFactory f = new FileSystemStorageFactory(
                            setup.getConfig(FileSystemStorageConfig::builder), setup.getExecutor());
                    return new ListenableStorageFactory(f, storage::set);
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                                                            this.zkClient, setup.getExecutor()));
    }

    //endregion
}
