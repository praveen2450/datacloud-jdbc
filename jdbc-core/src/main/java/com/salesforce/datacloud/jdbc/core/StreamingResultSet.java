/*
 * Copyright (c) 2024, Salesforce, Inc.
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
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler.createException;
import static com.salesforce.datacloud.jdbc.util.ArrowUtils.toColumnMetaData;

import com.salesforce.datacloud.jdbc.core.fsm.QueryResultIterator;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.TimeZone;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Slf4j
public class StreamingResultSet extends AvaticaResultSet implements DataCloudResultSet {
    private static final int ROOT_ALLOCATOR_MB_FROM_V2 = 100 * 1024 * 1024;

    @Getter
    private final String queryId;

    private final ArrowStreamReaderCursor cursor;
    ThrowingJdbcSupplier<DataCloudQueryStatus> getQueryStatus;

    private StreamingResultSet(
            ArrowStreamReaderCursor cursor,
            String queryId,
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame)
            throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        this.cursor = cursor;
        this.queryId = queryId;
    }

    public static StreamingResultSet of(QueryResultIterator iterator) throws DataCloudJDBCException {
        return of(iterator, iterator.getQueryId());
    }

    public static StreamingResultSet of(Iterator<QueryResult> iterator, String queryId) throws DataCloudJDBCException {
        try {
            val channel = new StreamingByteStringChannel(iterator);
            val reader = new ArrowStreamReader(channel, new RootAllocator(ROOT_ALLOCATOR_MB_FROM_V2));
            val schemaRoot = reader.getVectorSchemaRoot();
            val columns = toColumnMetaData(schemaRoot.getSchema().getFields());
            val timezone = TimeZone.getDefault();
            val state = new QueryState();
            val signature = new Meta.Signature(
                    columns, null, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
            val metadata = new AvaticaResultSetMetaData(null, null, signature);
            val cursor = new ArrowStreamReaderCursor(reader);
            val result = new StreamingResultSet(cursor, queryId, null, state, signature, metadata, timezone, null);
            result.execute2(cursor, columns);

            return result;
        } catch (Exception ex) {
            throw createException(QUERY_FAILURE + queryId, ex);
        }
    }

    private static final String QUERY_FAILURE = "Failed to execute query: ";

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getRow() {
        return cursor.getRowsSeen();
    }
}
