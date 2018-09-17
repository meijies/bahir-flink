/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kudu.connector;

import java.io.IOException;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncKuduConnector implements Connector {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private AsyncKuduClient client;
    private KuduTable table;
    private AsyncKuduSession session;
    private Operation operation;
    private long currentCount;
    private DefaultWindow defaultWindow;
    private WriteMode writeMode;

    public AsyncKuduConnector(String kuduMasters, KuduTableInfo tableInfo)
        throws IOException {
        client = client(kuduMasters);
        table = table(tableInfo);
        session = client.newSession();
    }

    public AsyncKuduConnector withWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
        operation = KuduMapper.toOperation(table, writeMode);
        return this;
    }

    public AsyncKuduConnector withDefaultWindow(DefaultWindow defaultWindow) {
        this.defaultWindow = defaultWindow;
        return this;
    }

    private AsyncKuduClient client(String kuduMasters) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters).build();
    }

    private KuduTable table(KuduTableInfo infoTable) throws IOException {
        KuduClient syncClient = client.syncClient();

        String tableName = infoTable.getName();
        if (syncClient.tableExists(tableName)) {
            return syncClient.openTable(tableName);
        }
        if (infoTable.createIfNotExist()) {
            return syncClient
                .createTable(tableName, infoTable.getSchema(), infoTable.getCreateTableOptions());
        }
        throw new UnsupportedOperationException(
            "table not exists and is marketed to not be created");
    }

    public boolean deleteTable() throws IOException {
        String tableName = table.getName();
        client.syncClient().deleteTable(tableName);
        return true;
    }

    public KuduScanner scanner(byte[] token) throws IOException {
        return KuduScanToken.deserializeIntoScanner(token, client.syncClient());
    }

    public List<KuduScanToken> scanTokens(List<KuduFilterInfo> tableFilters,
        List<String> tableProjections, Long rowLimit) {
        KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.syncClient()
            .newScanTokenBuilder(table);

        if (CollectionUtils.isNotEmpty(tableProjections)) {
            tokenBuilder.setProjectedColumnNames(tableProjections);
        }

        if (CollectionUtils.isNotEmpty(tableFilters)) {
            tableFilters.stream()
                .map(filter -> filter.toPredicate(table.getSchema()))
                .forEach(tokenBuilder::addPredicate);
        }

        if (rowLimit != null && rowLimit > 0) {
            tokenBuilder.limit(rowLimit);
            // FIXME: https://issues.apache.org/jira/browse/KUDU-16
            // Server side limit() operator for java-based scanners are not implemented yet
        }

        return tokenBuilder.build();
    }

    public void writeRow(KuduRow row, WriteMode writeMode) throws Exception {

        KuduMapper.addRow(operation, table, row);
        currentCount = currentCount + 1;
        if (defaultWindow.isPassed(currentCount)) {
            session.apply(operation);
            session.flush();
            operation = KuduMapper.toOperation(table, writeMode);
            processResponse(session.getPendingErrors().getRowErrors());
        }
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
        }
        if (client != null) {
            client.close();
        }
    }


    private void processResponse(RowError[] rowErrors) {
        for (RowError rowError : rowErrors) {
            logResponseError(rowError);
        }
    }

    private void logResponseError(RowError error) {
        LOG.error("Error {} on {}: {} ", error.getErrorStatus(), error.getOperation(),
            error.toString());
    }
}
