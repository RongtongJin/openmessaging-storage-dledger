/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.protocol;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import java.util.ArrayList;
import java.util.List;

public class PushEntryRequest extends RequestOrResponse {
    private long commitIndex = -1;
    private Type type = Type.APPEND;
    private DLedgerEntry entry;
    private int count = 0;
//    private long firstIndex = -1;
    private List<DLedgerEntry> entries = new ArrayList<>();

    public DLedgerEntry getEntry() {
        return entry;
    }

    public void setEntry(DLedgerEntry entry) {
        this.entry = entry;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void addEntry(DLedgerEntry entry) {
//        if (firstIndex == -1) {
//            firstIndex = entry.getIndex();
//            System.out.println("add Entry,first Index " + firstIndex);
//        }
        entries.add(entry);
        count++;
    }

    public List<DLedgerEntry> getEntries() {
        return entries;
    }

//    public long getFirstIndex() {
//        return firstIndex;
//    }

    public int getCount() {
        return count;
    }

    public void clearBatchRequest() {
        entries.clear();
//        firstIndex = -1;
        count = 0;
    }

    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE,
        BATCH_APPEND
    }
}
