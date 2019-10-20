package com.compose.nifi.processors;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by hayshutton on 7/22/16.
 * <p>
 * Started with the default GetMongo from the NiFi source nar bundle
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"compose", "mongodb", "streaming query"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "This is the content type for the content. Always equal to application/json."),
        @WritesAttribute(attribute = "mongo.id", description = "The MongoDB object_id for the document in hex format."),
        @WritesAttribute(attribute = "mongo.op", description = "The MongoDB operation to match other Processors. `q` for query is used. It is made up"),
        @WritesAttribute(attribute = "mongo.db", description = "The MongoDB db."),
        @WritesAttribute(attribute = "mongo.collection", description = "The MongoDB collection")
})
@CapabilityDescription("Streams documents from collection(s) instead of waiting for query to finish before next step.")
public class ComposeStreamingGetMongo extends AbstractMongoProcessor {
    private static final PropertyDescriptor COLLECTION_REGEX = new PropertyDescriptor.Builder()
            .name("Mongo Collection Regex")
            .description("The regex to match collections. Uses java.util regexes. The default of '.*' matches all collections")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final static Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All good documents go this way").build();

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    /**
     * Read the object from Base64 string.
     */
    private static Object fromString(String s) throws IOException,
            ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    /**
     * Write the object to a Base64 string.
     */
    private static String toString(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(COLLECTION_REGEX);
        _propertyDescriptors.add(JSON_TYPE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private Pattern userCollectionNamePattern;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnUnscheduled
    public final void interruptMainProcessorThread() {
        closeClient();
    }

    @OnScheduled
    public final void initPattern(ProcessContext context) {
        userCollectionNamePattern = Pattern.compile(context.getProperty(COLLECTION_REGEX).getValue());
    }


    private ArrayList<String> getUserCollectionNames(final String dbName) {
        ArrayList<String> userCollectionNames = new ArrayList<>();
        MongoIterable<String> names = mongoClient.getDatabase(dbName).listCollectionNames();
        for (String name : names) {
            if (userCollectionNamePattern.matcher(name).matches()) {
                userCollectionNames.add(name);
                getLogger().debug("Adding collectionName: {} due to match of {}", new Object[]{name,});
            }
        }
        return userCollectionNames;
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        final String dbName = context.getProperty(DATABASE_NAME).getValue();
        final String uri = context.getProperty(URI).getValue();

        configureMapper(jsonTypeSetting, null);

        for (String collectionName : getUserCollectionNames(dbName)) {
            if (MongoWrapper.systemIndexesPattern.matcher(collectionName).matches()) {
                continue;
            }

            final MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);

            try {
                final FindIterable<Document> it = collection.find();
                it.batchSize(context.getProperty(BATCH_SIZE).asInteger());
                final MongoCursor<Document> cursor = it.iterator();

                try {
                    while (cursor.hasNext()) {
                        FlowFile flowFile = session.create();

                        final Document currentDoc = cursor.next();
                        ObjectId currentObjectId = currentDoc.getObjectId("_id");

                        flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
                        flowFile = session.putAttribute(flowFile, "mongo.id", currentObjectId.toHexString());
                        flowFile = session.putAttribute(flowFile, "mongo.op", "q");
                        flowFile = session.putAttribute(flowFile, "mongo.db", dbName);
                        flowFile = session.putAttribute(flowFile, "mongo.collection", collectionName);

                        flowFile = session.write(flowFile, out -> {
                            if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                                out.write(objectMapper.writer().writeValueAsString(currentDoc).getBytes(StandardCharsets.UTF_8));
                            } else {
                                out.write(currentDoc.toJson().getBytes(StandardCharsets.UTF_8));
                            }
                        });

                        session.getProvenanceReporter().receive(flowFile, uri);
                        session.transfer(flowFile, REL_SUCCESS);
                        session.commit();
                    }
                } finally {
                    cursor.close();
                }
            } catch (final Throwable t) {
                getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
                throw new ProcessException(t);
            }
        }
    }
}
