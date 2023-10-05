package io.apicurio.registry.serde.jsonschema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.SchemaParser;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.strategy.ArtifactReferenceResolverStrategy;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.AbstractNATSSerializer;
import io.apicurio.registry.serde.headers.MessageTypeSerdeHeaders;
import java.io.IOException;
import java.io.OutputStream;
import io.nats.client.Schema;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

public class JSONSchemaNATSSerializer<T> extends AbstractNATSSerializer<JsonSchema, T> implements Schema<T> {
    private ObjectMapper mapper;
    private final JsonSchemaParser<T> parser = new JsonSchemaParser<>();

    private Boolean validationEnabled;
    private MessageTypeSerdeHeaders serdeHeaders;

    public JSONSchemaNATSSerializer() { super(); }

    public JSONSchemaNATSSerializer(RegistryClient client,
                                     ArtifactReferenceResolverStrategy<JsonSchema, T> artifactResolverStrategy,
                                     SchemaResolver<JsonSchema, T> schemaResolver) {
        super(client, artifactResolverStrategy, schemaResolver);
    }

    public JSONSchemaNATSSerializer(RegistryClient client) {
        super(client);
    }

    public JSONSchemaNATSSerializer(SchemaResolver<JsonSchema, T> schemaResolver) {
        super(schemaResolver);
    }

    public JSONSchemaNATSSerializer(RegistryClient client, Boolean validationEnabled) {
        this(client);
        this.validationEnabled = validationEnabled;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        JSONSchemaNATSSerializerConfig config = new JSONSchemaNATSSerializerConfig(configs);
        super.configure(config, isKey);
//
//        if (validationEnabled == null) {
//            this.validationEnabled = config.validationEnabled();
//        }
//
//        serdeHeaders = new MessageTypeSerdeHeaders(new HashMap<>(configs), isKey);

        if (null == mapper) {
            this.mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        }
    }

    public boolean isValidationEnabled() {
        return validationEnabled != null && validationEnabled;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.mapper = objectMapper;
    }

    @Override
    protected void serializeData(ParsedSchema<JsonSchema> schema, T message, OutputStream out) throws IOException {
        serializeData(null, schema, message, out);
    }

    public void setValidationEnabled(Boolean validationEnabled) {
        this.validationEnabled = validationEnabled;
    }

    @Override
    public SchemaParser<JsonSchema, T> schemaParser() {
        return parser;
    }

    @Override
    protected void serializeData(Headers headers, ParsedSchema<JsonSchema> schema, T message, OutputStream out) throws IOException {
        final byte[] dataBytes = mapper.writeValueAsBytes(message);

        out.write(dataBytes);
    }

}
