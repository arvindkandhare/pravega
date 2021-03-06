/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.notifier.SegmentNotifier;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.InlineExecutor;
import lombok.extern.slf4j.Slf4j;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class SegmentNotifierTest {

    @Spy
    private NotificationSystem system = new NotificationSystem();
    @Spy
    private ScheduledExecutorService executor = new InlineExecutor();
    @Mock
    private StateSynchronizer<ReaderGroupState> sync;
    @Mock
    private ReaderGroupState state;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("pravega.client.segmentNotification.poll.interval.seconds", String.valueOf(1));
    }

    @Test
    public void segmentNotifierTest() throws Exception {
        AtomicBoolean listenerInvoked = new AtomicBoolean();
        ReusableLatch latch = new ReusableLatch();

        when(state.getOnlineReaders()).thenReturn(new HashSet<>(singletonList("reader1")));
        when(state.getNumberOfSegments()).thenReturn(1).thenReturn(2);
        when(sync.getState()).thenReturn(state);

        Listener<SegmentNotification> listener1 = e -> {
            log.info("listener 1 invoked");
            listenerInvoked.set(true);
            latch.release();
        };
        Listener<SegmentNotification> listener2 = e -> {
        };

        SegmentNotifier notifier = new SegmentNotifier(system, sync, executor);
        notifier.registerListener(listener1);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        latch.await();
        assertTrue(listenerInvoked.get());

        notifier.registerListener(listener2);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        notifier.registerListener(listener1); //duplicate listener

        notifier.unregisterAllListeners();
        verify(system, times(1)).removeListeners(SegmentNotification.class.getSimpleName());
    }

    @After
    public void cleanup() {
        executor.shutdownNow();
    }
}
