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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @author meijie created at 9/17/18
 */
public class DefaultWindow implements Serializable {

    private static final long serialVersionUID = -8670298849963263760L;

    private long millisStep = Long.MAX_VALUE;
    private long end = Long.MAX_VALUE;
    private long count = Long.MAX_VALUE;

    public DefaultWindow() {
    }

    public DefaultWindow withTimeWindow(final long step, TimeUnit unit) {
        millisStep = unit.toMillis(step);
        end = System.currentTimeMillis() + millisStep;
        return this;
    }

    public DefaultWindow withCountWindow(final long count) {
        this.count = count;
        return this;
    }

    /**
     * @param realCount
     * @return
     */
    public boolean isPassed(long realCount) {

        if (Long.MAX_VALUE == count  && Long.MAX_VALUE == millisStep) {
            return true;
        }

        if (realCount > count || System.currentTimeMillis() > end) {
            end = System.currentTimeMillis() + millisStep;
            return true;
        }

        return false;
    }
}
