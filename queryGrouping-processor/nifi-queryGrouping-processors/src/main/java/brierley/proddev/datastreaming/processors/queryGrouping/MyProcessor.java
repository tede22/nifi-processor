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
package brierley.proddev.datastreaming.processors.queryGrouping;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.queryrecord.FlowFileTable;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Tags({"bp", "prodDev", "brierley", "query", "sql"})
@CapabilityDescription("Evaluates a flowfile and groups together records based on a configured key. It will output "
+ " a new flowfile per group found.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor GROUPING_KEY = new PropertyDescriptor
            .Builder().name("GROUPING_KEY")
            .displayName("grouping key")
            .description("Key used to group similar records together.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing results to a FlowFile")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile is routed to this relationship")
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A successful FlowFile is routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the SQL "
                    + "statement contains columns not present in input data), the original FlowFile it will "
                    + "be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(GROUPING_KEY);
        descriptors.add(RECORD_READER_FACTORY);
        descriptors.add(RECORD_WRITER_FACTORY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        if ( original == null ) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);

        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        //final Map<FlowFile, Relationship> transformedFlowFiles = new HashMap<>();
        final List<FlowFile> splits = new ArrayList<>();
        final Set<FlowFile> createdFlowFiles = new HashSet<>();

        // Determine the Record Reader's schema
        final RecordSchema readerSchema;
        try (final InputStream rawIn = session.read(original)) {
            final Map<String, String> originalAttributes = original.getAttributes();
            final RecordReader reader = recordReaderFactory.createRecordReader(originalAttributes, rawIn, getLogger());
            final RecordSchema inputSchema = reader.getSchema();

            readerSchema = recordSetWriterFactory.getSchema(originalAttributes, inputSchema);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Record Schema from {}; routing to failure", new Object[] {original, e});
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Determine the schema for writing the data
        final Map<String, String> originalAttributes = original.getAttributes();
        int recordsRead = 0;

        try {


            boolean flowFileRemoved = false;

            final String sql = "SELECT naturalKey FROM FLOWFILE GROUP BY naturalKey";
            final QueryResult queryResult;

            String test = context.getProperty(GROUPING_KEY).evaluateAttributeExpressions(original).getValue();



            queryResult = query(session, original, sql, context, recordReaderFactory);

            try {
                final ResultSet rs = queryResult.getResultSet();


                while (rs.next()) {
                    FlowFile transformed = session.create(original);
                    final String key = queryResult.getResultSet().getString(1);


                    final String groupSql = "SELECT * FROM FLOWFILE WHERE naturalKey = '" + key +"'";

                    getLogger().info("Query {}", new Object[] {groupSql});

                    final AtomicReference<WriteResult> writeResultRef = new AtomicReference<>();
                    final AtomicReference<String> mimeTypeRef = new AtomicReference<>();

                    final QueryResult groupResult = query(session, original, groupSql, context, recordReaderFactory);

                    try {

                        final ResultSet grs = groupResult.getResultSet();

                        transformed = session.write(transformed, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream outputStream) throws IOException {
                                final ResultSetRecordSet recordSet;
                                final RecordSchema writeSchema;
                                try {
                                    recordSet = new ResultSetRecordSet(grs, readerSchema);
                                    final RecordSchema resultSetSchema = recordSet.getSchema();
                                    writeSchema = recordSetWriterFactory.getSchema(originalAttributes, resultSetSchema);
                                }
                                catch (final SQLException | SchemaNotFoundException e) {
                                    throw new ProcessException(e);
                                }

                                try (final RecordSetWriter resultSetWriter = recordSetWriterFactory.createWriter(getLogger(), writeSchema, outputStream)){

                                    writeResultRef.set(resultSetWriter.write(recordSet));
                                    mimeTypeRef.set(resultSetWriter.getMimeType());

                                }
                                catch (final Exception e) {
                                    throw new IOException(e);
                                }


                            }
                        });

                    }
                    finally {
                        closeQuietly(groupResult);
                    }

                    recordsRead = Math.max(recordsRead, groupResult.getRecordsRead());
                    final WriteResult result = writeResultRef.get();
                    if(result.getRecordCount() == 0)
                    {
                        session.remove(transformed);
                        flowFileRemoved = true;
                        getLogger().info("The result contained no data.");

                    }
                    else {
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        if(result.getAttributes() != null) {
                            attributesToAdd.putAll(result.getAttributes());
                        }

                        attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());
                        attributesToAdd.put("record.count", String.valueOf(result.getRecordCount()));
                        transformed = session.putAllAttributes(transformed, attributesToAdd);
                        //transformedFlowFiles.put(transformed, REL_SUCCESS);
                        splits.add(transformed);

                        session.adjustCounter("Records Written", groupResult.getRecordsRead(), false);
                    }

                }
            }
            finally {


                closeQuietly(queryResult);
            }

            final long elapsedMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);

/*            if (transformedFlowFiles.size() > 0) {
                session.getProvenanceReporter().fork(original, transformedFlowFiles.keySet(), elapsedMillis);

                for (final Map.Entry<FlowFile, Relationship> entry : transformedFlowFiles.entrySet()) {
                    final FlowFile groupedFile = entry.getKey();
                    final Relationship relationship = entry.getValue();

                    //session.getProvenanceReporter().route(groupedFile, relationship);
                    //session.transfer(groupedFile, relationship);

                }

            }*/

            session.transfer(splits, REL_SUCCESS);


            getLogger().info("Successfully queried {} in {} millis", new Object[] {original, elapsedMillis});
            session.transfer(original, REL_ORIGINAL);
        } catch (final SQLException e) {
            getLogger().error("Unable to query {} due to {}", new Object[] {original, e.getCause() == null ? e : e.getCause()});
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("Unable to query {} due to {}", new Object[] {original, e});
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);

        }

        session.adjustCounter("Records Read", recordsRead, false);


    }



    protected QueryResult query(final ProcessSession session, final FlowFile flowFile, final String sql, final ProcessContext context,
                                final RecordReaderFactory recordParserFactory) throws SQLException {

        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

        Connection connection = null;
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection("jdbc:calcite:", properties);
            final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            final SchemaPlus rootSchema = calciteConnection.getRootSchema();

            final FlowFileTable<?, ?> flowFileTable = new FlowFileTable<>(session, flowFile, recordParserFactory, getLogger());
            rootSchema.add("FLOWFILE", flowFileTable);
            rootSchema.setCacheEnabled(false);

            statement = connection.createStatement();

            try {
                resultSet = statement.executeQuery(sql);
            } catch (final Throwable t) {
                flowFileTable.close();
                throw t;
            }

            final ResultSet rs = resultSet;
            final Statement stmt = statement;
            final Connection conn = connection;

            return new QueryResult() {
                @Override
                public void close() throws IOException {
                    closeQuietly(rs, stmt, conn);
                }

                @Override
                public ResultSet getResultSet() {
                    return rs;
                }

                @Override
                public int getRecordsRead() {
                    return flowFileTable.getRecordsRead();
                }
            };
        } catch (final Exception e) {
            closeQuietly(resultSet, statement, connection);
            throw e;
        }
    }

    private void closeQuietly(final AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }

        for (final AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close SQL resource", e);
            }
        }
    }

    private static interface QueryResult extends Closeable {
        ResultSet getResultSet();

        int getRecordsRead();
    }




}
