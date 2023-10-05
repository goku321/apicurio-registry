package io.apicurio.registry.serde.jsonschema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.SchemaParser;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.utils.Utils;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.AbstractNATSDeserializer;
import io.apicurio.registry.serde.headers.MessageTypeSerdeHeaders;
import io.nats.client.Schema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

public class JSONSchemaNATSDeserializer<T> extends AbstractNATSDeserializer<JsonSchema, T> implements Schema<T> {
    private ObjectMapper mapper;
    private Boolean validationEnabled;
    private JsonSchemaParser<T> parser = new JsonSchemaParser<>();

    /**
     * Optional, the full class name of the java class to deserialize
     */
    private Class<T> specificReturnClass;
    private MessageTypeSerdeHeaders serdeHeaders;

    public JSONSchemaNATSDeserializer() {
        super();
    }

    public JSONSchemaNATSDeserializer(RegistryClient client,
                                       SchemaResolver<JsonSchema, T> schemaResolver) {
        super(client, schemaResolver);
    }

    public JSONSchemaNATSDeserializer(RegistryClient client) {
        super(client);
    }

    public JSONSchemaNATSDeserializer(SchemaResolver<JsonSchema, T> schemaResolver) {
        super(schemaResolver);
    }

    public JSONSchemaNATSDeserializer(RegistryClient client, Boolean validationEnabled) {
        this(client);
        this.validationEnabled = validationEnabled;
    }

    /**
     * @see io.apicurio.registry.serde.AbstractNATSDeserializer#configure(java.util.Map, boolean)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        JSONSchemaNATSDeserializerConfig config = new JSONSchemaNATSDeserializerConfig(configs);
        super.configure(config, isKey);

//        if (validationEnabled == null) {
//            this.validationEnabled = config.validationEnabled();
//        }

        this.specificReturnClass = (Class<T>) config.getSpecificReturnClass();

        this.serdeHeaders = new MessageTypeSerdeHeaders(new HashMap<>(configs), isKey);

        if (null == mapper) {
            mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);;
        }
    }

    public boolean isValidationEnabled() {
        return validationEnabled != null && validationEnabled;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.mapper = objectMapper;
    }

    /**
     * @see io.apicurio.registry.serde.AbstractKafkaSerDe#schemaParser()
     */
    @Override
    public SchemaParser<JsonSchema, T> schemaParser() {
        return parser;
    }

    /**
     * @see io.apicurio.registry.serde.AbstractNATSDeserializer#readData(ParsedSchema, ByteBuffer, int, int)
     */
    @Override
    protected T readData(ParsedSchema<JsonSchema> schema, ByteBuffer buffer, int start, int length) {
        return internalReadData(null, schema, buffer, start, length);
    }

    /**
     * @see io.apicurio.registry.serde.AbstractNATSDeserializer#readData(Headers, ParsedSchema, ByteBuffer, int, int)
     */
    @Override
    protected T readData(Headers headers, ParsedSchema<JsonSchema> schema, ByteBuffer buffer, int start, int length) {
        return internalReadData(headers, schema, buffer, start, length);
    }

    private T internalReadData(Headers headers, ParsedSchema<JsonSchema> schema, ByteBuffer buffer, int start, int length) {
        byte[] data = new byte[length];
        System.arraycopy(buffer.array(), start, data, 0, length);

        try {
            JsonParser parser = mapper.getFactory().createParser(data);

            if (isValidationEnabled()) {
                JsonSchemaValidationUtil.validateDataWithSchema(schema, data, mapper);
            }

            Class<T> messageType = null;

            if (this.specificReturnClass != null) {
                messageType = this.specificReturnClass;
            } else if (headers == null) {
                JsonNode jsonSchema = mapper.readTree(schema.getRawSchema());

                String javaType = null;
                JsonNode javaTypeNode = jsonSchema.get("javaType");
                if (javaTypeNode != null && !javaTypeNode.isNull()) {
                    javaType = javaTypeNode.textValue();
                }
                //TODO if javaType is null, maybe warn something like this?
                //You can try configure the property \"apicurio.registry.serde.json-schema.java-type\" with the full class name to use for deserialization
                messageType = javaType == null ? null : Utils.loadClass(javaType);
            } else {
                String javaType = serdeHeaders.getMessageType(headers);
                messageType = javaType == null ? null : Utils.loadClass(javaType);
            }

            if (messageType == null) {
                //TODO maybe warn there is no message type and the deserializer will return a JsonNode
                return mapper.readTree(parser);
            } else {
                return mapper.readValue(parser, messageType);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
