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
package org.apache.flink.streaming.connectors.kudu;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kudu.connector.AsyncKuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.Connector;
import org.apache.flink.streaming.connectors.kudu.connector.Consistency;
import org.apache.flink.streaming.connectors.kudu.connector.DefaultWindow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.streaming.connectors.kudu.connector.WriteMode;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KuduOutputFormat<OUT extends KuduRow> implements OutputFormat<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);
    private static final long serialVersionUID = 2889146067231385376L;

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private Consistency consistency;
    private WriteMode writeMode;
    private DefaultWindow defaultWindow = new DefaultWindow();

    private transient Connector tableContext;


    public KuduOutputFormat(String kuduMasters, KuduTableInfo tableInfo) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = Consistency.STRONG;
        this.writeMode = WriteMode.UPSERT;
    }

    public KuduOutputFormat<OUT> withEventualConsistency() {
        this.consistency = Consistency.EVENTUAL;
        return this;
    }

    public KuduOutputFormat<OUT> withStrongConsistency() {
        this.consistency = Consistency.STRONG;
        return this;
    }

    public KuduOutputFormat<OUT> withUpsertWriteMode() {
        this.writeMode = WriteMode.UPSERT;
        return this;
    }

    public KuduOutputFormat<OUT> withInsertWriteMode() {
        this.writeMode = WriteMode.INSERT;
        return this;
    }

    public KuduOutputFormat<OUT> withUpdateWriteMode() {
        this.writeMode = WriteMode.UPDATE;
        return this;
    }

    public KuduOutputFormat<OUT> withCountWindow(final long count) {
        defaultWindow.withCountWindow(count);
        return this;
    }

    public KuduOutputFormat<OUT> withTimeWindow(final long time, final TimeUnit unit) {
        defaultWindow.withTimeWindow(time, unit);
        return this;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        startTableContext();
    }

    private void startTableContext() throws IOException {
        if (tableContext != null) return;
        if (Consistency.EVENTUAL.equals(consistency)) {
            tableContext = new AsyncKuduConnector(kuduMasters, tableInfo)
            .withWriteMode(writeMode)
            .withDefaultWindow(defaultWindow);
        } else {
            tableContext = new KuduConnector(kuduMasters, tableInfo);
        }
    }

    @Override
    public void writeRecord(OUT kuduRow) throws IOException {
        try {
            tableContext.writeRow(kuduRow, writeMode);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.tableContext == null) return;
        try {
            this.tableContext.close();
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }
}
