package io.apicurio.registry.serde;

import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.SchemaLookupResult;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.strategy.ArtifactReferenceResolverStrategy;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.config.BaseNATSSerDeConfig;
import io.apicurio.registry.serde.data.NATSSerdeMetadata;
import io.apicurio.registry.serde.data.NATSSerdeRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import io.nats.client.Schema;

public abstract class AbstractNATSSerializer<T, U> extends AbstractNATSSerDe<T, U> implements Schema<U> {
    public AbstractNATSSerializer() {
        super();
    }

    public AbstractNATSSerializer(RegistryClient client) {
        super(client);
    }

    public AbstractNATSSerializer(io.apicurio.registry.resolver.SchemaResolver<T, U> schemaResolver) {
        super(schemaResolver);
    }

    public AbstractNATSSerializer(RegistryClient client, ArtifactReferenceResolverStrategy<T, U> artifactResolverStrategy, SchemaResolver<T, U> schemaResolver) {
        super(client, schemaResolver);
        getSchemaResolver().setArtifactResolverStrategy(artifactResolverStrategy);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(new BaseNATSSerDeConfig(configs), isKey);
    }

    protected abstract void serializeData(io.apicurio.registry.resolver.ParsedSchema<T> schema, U message, OutputStream out) throws
            IOException;

    protected abstract void serializeData(Headers headers, ParsedSchema<T> schema, U message, OutputStream out) throws IOException;

    @Override
    public byte[] encode(U message) {
        // just return null
        if (message == null) {
            return null;
        }
        try {

            NATSSerdeMetadata resolverMetadata = new NATSSerdeMetadata("subject", isKey(), null);

            SchemaLookupResult<T>
                    schema = getSchemaResolver().resolveSchema(new NATSSerdeRecord<>(resolverMetadata, message));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (headersHandler != null) {
                headersHandler.writeHeaders(null, schema.toArtifactReference());
                serializeData(null, schema.getParsedSchema(), message, out);
            } else {
                out.write(MAGIC_BYTE);
                getIdHandler().writeId(schema.toArtifactReference(), out);
                serializeData(schema.getParsedSchema(), message, out);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public U decode(String subject, byte[] bytes) {
        throw new UncheckedIOException(new IOException());
    }
}
