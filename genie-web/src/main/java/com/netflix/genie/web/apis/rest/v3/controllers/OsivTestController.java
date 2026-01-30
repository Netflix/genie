/*
 *
 *  Copyright 2026 Netflix, Inc.
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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.netflix.genie.web.data.services.PersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Test controller to demonstrate OSIV connection pool behavior.
 * For dev testing only - DO NOT MERGE.
 */
@RestController
@RequestMapping("/api/v3/test/osiv")
@Slf4j
public class OsivTestController {

    private final DataSource dataSource;
    private final PersistenceService persistenceService;

    @Value("${genie.test.osiv.poll-duration-ms:120000}")
    private long pollDurationMs;

    @Value("${genie.test.osiv.poll-interval-ms:1000}")
    private long pollIntervalMs;

    /**
     * Constructor.
     *
     * @param dataSource          The datasource
     * @param persistenceService  The persistence service
     */
    public OsivTestController(final DataSource dataSource, final PersistenceService persistenceService) {
        this.dataSource = dataSource;
        this.persistenceService = persistenceService;
    }

    /**
     * Test endpoint to demonstrate OSIV connection pool behavior.
     *
     * @return Connection pool state before, during, and after query
     */
    @GetMapping(value = "/simple-test", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> simpleTest() {
        log.info("=== OSIV TEST START ===");
        final Map<String, Object> result = new HashMap<>();

        result.put("beforeQuery", getConnectionPoolState());
        log.info("BEFORE-QUERY: {}", result.get("beforeQuery"));

        try {
            final long count = this.persistenceService.getActiveJobCountForUser("testuser");
            result.put("jobCount", count);
        } catch (Exception e) {
            log.warn("Query failed (expected): {}", e.getMessage());
        }

        result.put("afterQuery", getConnectionPoolState());
        log.info("AFTER-QUERY: {}", result.get("afterQuery"));

        log.info("Polling for {} ms with interval {} ms", pollDurationMs, pollIntervalMs);
        final long endTime = System.currentTimeMillis() + pollDurationMs;
        int pollCount = 0;
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(pollIntervalMs);
                pollCount++;
                final Map<String, Integer> state = getConnectionPoolState();
                log.info("POLL-{}: {}", pollCount, state);
                result.put("poll" + pollCount, state);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        result.put("beforeReturn", getConnectionPoolState());
        log.info("BEFORE-RETURN: {}", result.get("beforeReturn"));
        log.info("=== OSIV TEST END ===");

        return result;
    }

    private Map<String, Integer> getConnectionPoolState() {
        final Map<String, Integer> state = new HashMap<>();
        if (dataSource instanceof HikariDataSource) {
            final HikariPoolMXBean pool = ((HikariDataSource) dataSource).getHikariPoolMXBean();
            if (pool != null) {
                state.put("active", pool.getActiveConnections());
                state.put("idle", pool.getIdleConnections());
                state.put("total", pool.getTotalConnections());
                state.put("waiting", pool.getThreadsAwaitingConnection());
            }
        }
        return state;
    }
}
