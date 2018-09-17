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
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduConnector implements Connector {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private KuduClient client;
    private KuduTable table;
    private KuduSession session;
    private WriteMode writeMode;

    public KuduConnector(String kuduMasters, KuduTableInfo tableInfo)
        throws IOException {
        client = client(kuduMasters);
        table = table(tableInfo);
        session = client.newSession();
    }

    public KuduConnector withWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
        return this;
    }


    private KuduClient client(String kuduMasters) {
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    private KuduTable table(KuduTableInfo infoTable) throws IOException {

        String tableName = infoTable.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        if (infoTable.createIfNotExist()) {
            return client
                .createTable(tableName, infoTable.getSchema(), infoTable.getCreateTableOptions());
        }
        throw new UnsupportedOperationException(
            "table not exists and is marketed to not be created");
    }

    public boolean deleteTable() throws IOException {
        String tableName = table.getName();
        client.deleteTable(tableName);
        return true;
    }

    public KuduScanner scanner(byte[] token) throws IOException {
        return KuduScanToken.deserializeIntoScanner(token, client);
    }

    public List<KuduScanToken> scanTokens(List<KuduFilterInfo> tableFilters,
        List<String> tableProjections, Long rowLimit) {
        KuduScanToken.KuduScanTokenBuilder tokenBuilder = client
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

    public void writeRow(KuduRow row) throws KuduException {
        final Operation operation = KuduMapper.toOperation(table, writeMode, row);
        session.apply(operation);
        session.flush();
        processResponse(session.getPendingErrors().getRowErrors());
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
