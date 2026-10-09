/*******************************************************************************
 * Copyright (c) 2022, 2026 Eurotech and/or its affiliates and others
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Eurotech
 ******************************************************************************/

package org.eclipse.kura.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.core.data.AutoConnectStrategy.ConnectionManager;
import org.eclipse.kura.message.store.StoredMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.quartz.CronExpression;

class ScheduleStrategyTest {

    private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    private final ConnectionManager manager = mock(ConnectionManager.class);
    private ScheduleStrategy strategy;
    private Runnable timeout;
    private long delay;
    private ScheduledFuture<?> pending;
    private CronExpression expression;
    private DataServiceOptions options;

    @BeforeEach
    void setup() throws Exception {
        expression = new CronExpression("0/2 * * * * ?");
        expression.setTimeZone(TimeZone.getTimeZone("UTC"));
        options = new DataServiceOptions(Map.of(
                "connection.schedule.priority.override.enable", true,
                "connection.schedule.priority.override.threshold", 3,
                "connection.schedule.enabled", true,
                "connection.schedule.expression", expression.getCronExpression(),
                "connection.schedule.inactivity.interval.seconds", 60L));
        when(manager.getNextMessage()).thenReturn(Optional.empty());
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(executor).execute(any());
        when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            timeout = invocation.getArgument(0);
            delay = invocation.getArgument(1);
            assertEquals(TimeUnit.MILLISECONDS, invocation.getArgument(2));
            pending = mock(ScheduledFuture.class);
            return pending;
        });
        when(executor.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);
    }

    @AfterEach
    void shutdown() throws Exception {
        if (strategy != null) {
            strategy.shutdown();
            verify(executor).shutdown();
            verify(executor).awaitTermination(30, TimeUnit.SECONDS);
            verify(pending, org.mockito.Mockito.atLeastOnce()).cancel(false);
        }
    }

    @Test
    void shouldScheduleFirstConnectionAttempt() {
        createStrategy();
        assertEquals(2000, delay);
    }

    @Test
    void shouldRequestConnection() {
        createStrategy();
        fireTimeout();
        verify(manager).startConnectionTask();
    }

    @Test
    void shouldScheduleDisconnectTimeout() {
        connect();
        assertEquals(60_000, delay);
    }

    @Test
    void shouldRequestDisconnect() {
        connect();
        fireTimeout();
        verify(manager).stopConnectionTask();
        verify(manager).disconnect();
    }

    @Test
    void shouldForceReconnectOutsideOfSchedule() {
        // Exercise the public constructor without creating an unowned scheduler thread.
        try (MockedStatic<Executors> factories = mockStatic(Executors.class)) {
            factories.when(Executors::newSingleThreadScheduledExecutor).thenReturn(executor);
            strategy = new ScheduleStrategy(expression, options, manager);
            factories.verify(Executors::newSingleThreadScheduledExecutor);
        }
        strategy.onPublishRequested("test/topic", null, 0, false, 7);
        verify(manager, never()).startConnectionTask();
        strategy.onPublishRequested("test/topic", null, 0, false, 0);
        verify(manager).startConnectionTask();
    }

    @Test
    void shouldReconnectIfMessageIsSentDuringDisconnect() {
        connect();
        fireTimeout();
        verify(manager).disconnect();
        StoredMessage priorityMessage = mock(StoredMessage.class);
        when(priorityMessage.getPriority()).thenReturn(0);
        when(manager.getNextMessage()).thenReturn(Optional.of(priorityMessage));
        strategy.onPublishRequested("test/topic", null, 0, false, 0);
        verify(manager).startConnectionTask();
        strategy.onDisconnected();
        verify(manager, times(2)).startConnectionTask();
    }

    private void createStrategy() {
        strategy = new ScheduleStrategy(expression, 60_000, manager, executor,
                () -> Date.from(Instant.parse("2000-01-01T00:00:00Z")), options);
    }

    private void connect() {
        createStrategy();
        fireTimeout();
        strategy.onConnectionEstablished();
    }

    private void fireTimeout() {
        assertNotNull(timeout);
        Runnable scheduled = timeout;
        timeout = null;
        scheduled.run();
    }
}
