/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.client;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.table.DingoKeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;

import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class StoreOperationUtils {

    private static Map<String, RouteTable> dingoRouteTables = new ConcurrentHashMap<>(127);
    private static Map<String, TableDefinition> tableDefinitionInCache = new ConcurrentHashMap<>(127);

    private static final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private DingoConnection connection;
    private int retryTimes;

    public StoreOperationUtils(DingoConnection connection, int retryTimes) {
        this.connection = connection;
        this.retryTimes = retryTimes;
    }

    public void shutdown() {
        executorService.shutdown();
        clearTableDefinitionInCache();
    }


    /**
     * get route table from coordinator or cache, and refresh it if need.
     * @param tableName input tableName
     * @param isRefresh if isRefresh == true, then refresh route table
     * @return RouteTable about table partition leaders and replicas on hosts.
     */
    public synchronized RouteTable getAndRefreshRouteTable(final String tableName, boolean isRefresh) {
        if (isRefresh) {
            dingoRouteTables.remove(tableName);
        }
        RouteTable routeTable = dingoRouteTables.get(tableName);
        if (routeTable == null) {
            MetaClient metaClient = connection.getMetaClient();
            CommonId tableId = metaClient.getTableId(tableName);
            TableDefinition tableDef = metaClient.getTableDefinition(tableName);
            if (tableDef == null) {
                log.error("Cannot find table:{} defination from meta", tableName);
                return null;
            }
            tableDefinitionInCache.put(tableName, tableDef);
            NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaClient.getParts(tableName);
            KeyValueCodec keyValueCodec = new DingoKeyValueCodec(tableDef.getDingoType(), tableDef.getKeyMapping());
            RangeStrategy rangeStrategy = new RangeStrategy(tableDef, partitions.navigableKeySet());
            routeTable = new RouteTable(
                tableName,
                tableId,
                keyValueCodec,
                partitions,
                rangeStrategy);
            dingoRouteTables.put(tableName, routeTable);
            log.info("Refresh route table:{}, tableDef:{}", tableName, tableDef);
        }
        return routeTable;
    }

    /**
     * get column name in order by index from table definition.
     * @param tableName input table name.
     * @return column name in order by index.
     */
    public synchronized TableDefinition getTableDefinition(String tableName) {
        TableDefinition tableDef = tableDefinitionInCache.get(tableName);
        if (tableDef == null) {
            MetaClient metaClient = connection.getMetaClient();
            tableDef = metaClient.getTableDefinition(tableName);
            if (tableDef != null) {
                tableDefinitionInCache.put(tableName, tableDef);
            }
        }

        if (tableDef == null) {
            log.error("Cannot find table:{} definition from meta", tableName);
            return null;
        }
        return tableDef;
    }

    /**
     * update table definition into local cache.
     * @param tableName table name.
     * @param tableDef table definition.
     */
    public synchronized void updateCacheOfTableDefinition(final String tableName, final TableDefinition tableDef) {
        if (tableName != null && !tableName.isEmpty() && tableDef != null) {
            tableDefinitionInCache.put(tableName, tableDef);
            log.info("update cache of table:{} definition:{}", tableName, tableDef);
        }
    }

    /**
     * remove table definition from local cache.
     * @param tableName input table name.
     */
    public synchronized void removeCacheOfTableDefinition(String tableName) {
        if (tableName != null) {
            TableDefinition tableDefinition = tableDefinitionInCache.remove(tableName);
            if (tableDefinition != null) {
                log.info("remove cache of table:{} definition:{}", tableName, tableDefinition);
            }
        }
    }

    public static Map<String, TableDefinition> getTableDefinitionInCache() {
        return tableDefinitionInCache;
    }

    private synchronized void clearTableDefinitionInCache() {
        tableDefinitionInCache.clear();
        return;
    }

    private synchronized ExecutorApi getExecutor(ApiRegistry apiRegistry, final RouteTable routeTable, String leaderAddress) {
        ExecutorApi executorApi = routeTable.getLeaderAddress(apiRegistry, leaderAddress);
        return executorApi;
    }

    private synchronized String getLeaderAddressByStartKey(final RouteTable routeTable, byte[] keyInBytes) {
        return routeTable.getStartPartitionKey(keyInBytes);
    }
}
