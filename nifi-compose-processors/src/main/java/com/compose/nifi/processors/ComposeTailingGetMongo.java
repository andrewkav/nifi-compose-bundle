package com.compose.nifi.processors;


import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.gt;

/**
 * Created by hayshutton on 8/25/16.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"compose", "mongodb", "get", "tailing"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "This is the content-type for the content."),
        @WritesAttribute(attribute = "mongo.id", description = "The MongoDB object_id for the document in hex format or the 'h' from the oplog document."),
        @WritesAttribute(attribute = "mongo.ts", description = "Timestamp of operation from oplog or timestamp of query prior to tailing."),
        @WritesAttribute(attribute = "mongo.op", description = "The Mongo operation. `i' for insert, 'd' for delete, 'u' for update, 'q' which is a placeholder for query result when not an oplog operation"),
        @WritesAttribute(attribute = "mongo.db", description = "The Mongo database name"),
        @WritesAttribute(attribute = "mongo.collection", description = "The Mongo collection name")
})
@Stateful(description = "Stores the timestamp of the latest OP log tx", scopes = Scope.LOCAL)
@CapabilityDescription("Dumps documents from a MongoDB and then dumps operations from the oplog in soft real time. The FlowFile content is the document itself from the find or the `o` attribute from the oplog. It keeps a connection open and waits on new oplog entries. Restart does the full dump again and then oplog tailing.")
public class ComposeTailingGetMongo extends AbstractMongoProcessor {
    private static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("the happy path for mongo documents and operations").build();

    private static final Set<Relationship> relationships;

    private static final List<PropertyDescriptor> propertyDescriptors;
    public static final String LATEST_TS = "latest_ts";

    private boolean isRunning;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(BATCH_SIZE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }


    @Override
    public final Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnUnscheduled
    public final void stopMainProcessorThread() {
        isRunning = false;
    }

    @OnScheduled
    public final void setRunning() {
        isRunning = true;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        final String databaseName = context.getProperty(DATABASE_NAME).getValue();
        final String uri = context.getProperty(URI).getValue();

        final Pattern collectionNamePattern = Pattern.compile(context.getProperty(COLLECTION_NAME).getValue());

        configureMapper(jsonTypeSetting, null);

        BsonTimestamp bts = new BsonTimestamp(0, 0);

        MongoCollection<Document> oplog = mongoClient.getDatabase("local").getCollection("oplog.rs");
        try {
            String latestTs = context.getStateManager().getState(Scope.LOCAL).get(LATEST_TS);
            int latestTsInt = 0;
            if (StringUtils.isNumeric(latestTs)) {
                latestTsInt = Integer.parseInt(latestTs);
                bts = new BsonTimestamp(latestTsInt, 0);
            }


            final FindIterable<Document> it = oplog.find(gt("ts", bts)).cursorType(CursorType.TailableAwait).oplogReplay(true).maxAwaitTime(5, TimeUnit.SECONDS);
            it.batchSize(context.getProperty(BATCH_SIZE).asInteger());
            final MongoCursor<Document> cursor = it.iterator();
            Document currentDoc = null;
            try {
                currentDoc = cursor.tryNext();
                int currentCount = 0;
                for (; isRunning; currentDoc = cursor.tryNext(), currentCount++) {
                    if (currentDoc == null) {
                        getLogger().debug("no new documents found, reiterating");
                        continue;
                    }

                    String[] namespace = currentDoc.getString("ns").split(Pattern.quote("."), 2);
                    if (databaseName.equals(namespace[0]) && collectionNamePattern.matcher(namespace[1]).matches()) {
                        FlowFile flowFile = session.create();

                        flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
                        flowFile = session.putAttribute(flowFile, "mongo.id", getId(currentDoc));
                        flowFile = session.putAttribute(flowFile, "mongo.op", currentDoc.getString("op"));
                        flowFile = session.putAttribute(flowFile, "mongo.db", databaseName);
                        flowFile = session.putAttribute(flowFile, "mongo.collection", namespace[1]);

                        final Document docRef = currentDoc;
                        flowFile = session.write(flowFile, out -> {
                            if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                                out.write(objectMapper.writer().writeValueAsString(docRef).getBytes(StandardCharsets.UTF_8));
                            } else {
                                out.write(docRef.toJson().getBytes(StandardCharsets.UTF_8));
                            }
                        });

                        session.getProvenanceReporter().receive(flowFile, uri);
                        session.transfer(flowFile, REL_SUCCESS);

                        session.commit();

                        latestTsInt = Math.max(latestTsInt, currentDoc.get("ts", BsonTimestamp.class).getTime());

                        // periodically save latest_ts
                        if (currentCount > 100) {
                            currentCount = 0;
                            getLogger().debug("saving new latest_ts = {}", new Object[]{latestTsInt});
                            context.getStateManager().setState(Collections.singletonMap(LATEST_TS, Integer.valueOf(latestTsInt).toString()), Scope.LOCAL);
                        }
                    }
                }
            } catch (ProcessException pe) {
                getLogger().error("unable to process the record, doc={}, error={}", new Object[]{currentDoc, pe});
            } finally {
                getLogger().debug("saving new latest_ts = {}", new Object[]{latestTsInt});
                context.getStateManager().setState(Collections.singletonMap(LATEST_TS, Integer.valueOf(latestTsInt).toString()), Scope.LOCAL);

                cursor.close();
            }
        } catch (Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back", new Object[]{this, t});
        }
    }

    private String getId(Document doc) {
        switch (doc.getString("op")) {
            case "i":
            case "d":
            case "u":
                Document o = doc.get("o", Document.class);
                return o != null && o.getObjectId("_id") != null ? o.getObjectId("_id").toHexString() : "NA";
            case "n":
            case "c":
                return Long.toString(doc.getLong("h"));
            default:
                return "NA";
        }
    }
}
