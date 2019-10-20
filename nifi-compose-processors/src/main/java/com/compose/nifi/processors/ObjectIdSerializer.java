package com.compose.nifi.processors;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.bson.types.ObjectId;

import java.io.IOException;

public class ObjectIdSerializer extends JsonSerializer<ObjectId> {
    @Override
    public void serialize(ObjectId objectId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeString(objectId.toString());
    }

    public static SimpleModule getModule() {
        SimpleModule module = new SimpleModule("ObjectID Serializer", Version.unknownVersion());
        ObjectIdSerializer serializer = new ObjectIdSerializer();
        module.addSerializer(ObjectId.class, serializer);

        return module;
    }
}