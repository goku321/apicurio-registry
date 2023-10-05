package io.apicurio.registry.serde;

import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.SchemaLookupResult;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.config.BaseNATSSerDeConfig;
import io.apicurio.registry.serde.fallback.DefaultFallbackArtifactProvider;
import io.apicurio.registry.serde.fallback.FallbackArtifactProvider;
import io.nats.client.Schema;
import java.nio.ByteBuffer;
import org.apache.kafka.common.header.Headers;

public abstract class AbstractNATSDeserializer<T, U> extends AbstractNATSSerDe<T, U> implements Schema<U> {
    protected FallbackArtifactProvider fallbackArtifactProvider;

    public AbstractNATSDeserializer() {
        super();
    }

    public AbstractNATSDeserializer(RegistryClient client) {
        super(client);
    }

    public AbstractNATSDeserializer(io.apicurio.registry.resolver.SchemaResolver<T, U> schemaResolver) {
        super(schemaResolver);
    }

    public AbstractNATSDeserializer(RegistryClient client, SchemaResolver<T, U> schemaResolver) {
        super(client, schemaResolver);
    }

    /**
     * @see io.apicurio.registry.serde.AbstractNATSSerDe#configure(io.apicurio.registry.serde.config.BaseNATSSerDeConfig, boolean)
     */
    @Override
    protected void configure(BaseNATSSerDeConfig config, boolean isKey) {
        super.configure(config, isKey);

        this.fallbackArtifactProvider = new DefaultFallbackArtifactProvider();
        this.fallbackArtifactProvider.configure(config.originals(), isKey);


//        BaseNATSSerDeConfig deserializerConfig = new BaseNATSSerDeConfig(config.originals());
////
//        Object fallbackProvider = deserializerConfig.getFallbackArtifactProvider();
//        Utils.instantiate(FallbackArtifactProvider.class, fallbackProvider, this::setFallbackArtifactProvider);
//        fallbackArtifactProvider.configure(config.originals(), isKey);
//
//        if (fallbackArtifactProvider instanceof DefaultFallbackArtifactProvider) {
//            if (!((DefaultFallbackArtifactProvider) fallbackArtifactProvider).isConfigured()) {
//                //it's not configured, just remove it so it's not executed
//                fallbackArtifactProvider = null;
//            }
//        }

    }

    /**
     * @param fallbackArtifactProvider the fallbackArtifactProvider to set
     */
    public void setFallbackArtifactProvider(FallbackArtifactProvider fallbackArtifactProvider) {
        this.fallbackArtifactProvider = fallbackArtifactProvider;
    }

    protected abstract U readData(io.apicurio.registry.resolver.ParsedSchema<T> schema, ByteBuffer buffer, int start, int length);

    protected abstract U readData(Headers headers, ParsedSchema<T> schema, ByteBuffer buffer, int start, int length);

    @Override
    public byte[] encode(U message) {
        return null;
    }

    @Override
    public U decode(String subject, byte[] data) {
        if (data == null) {
            return null;
        }

        ByteBuffer buffer = getByteBuffer(data);
        ArtifactReference artifactReference = getIdHandler().readId(buffer);

        io.apicurio.registry.resolver.SchemaLookupResult<T> schema = resolve(subject, null, data, artifactReference);

        int length = buffer.limit() - 1 - getIdHandler().idSize();
        int start = buffer.position() + buffer.arrayOffset();

        return readData(schema.getParsedSchema(), buffer, start, length);
    }

//    @Override
//    public U deserialize(String subject, Headers headers, byte[] data) {
//        if (data == null) {
//            return null;
//        }
//        ArtifactReference artifactReference = null;
//        if (headersHandler != null && headers != null) {
//            artifactReference = headersHandler.readHeaders(headers);
//
//            if (artifactReference.hasValue()) {
//                return readData(subject, headers, data, artifactReference);
//            }
//        }
//        if (data[0] == MAGIC_BYTE) {
//            return deserialize(subject, data);
//        } else if (headers == null){
//            throw new IllegalStateException("Headers cannot be null");
//        } else {
//            //try to read data even if artifactReference has no value, maybe there is a fallbackArtifactProvider configured
//            return readData(subject, headers, data, artifactReference);
//        }
//    }

    private U readData(String topic, Headers headers, byte[] data, ArtifactReference artifactReference) {
        io.apicurio.registry.resolver.SchemaLookupResult<T> schema = resolve(topic, headers, data, artifactReference);

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int length = buffer.limit();
        int start = buffer.position();

        return readData(headers, schema.getParsedSchema(), buffer, start, length);
    }

    private SchemaLookupResult<T> resolve(String topic, Headers headers, byte[] data, ArtifactReference artifactReference) {
        try {
            return getSchemaResolver().resolveSchemaByArtifactReference(artifactReference);
        } catch (RuntimeException e) {
            if (fallbackArtifactProvider == null) {
                throw e;
            } else {
                try {
                    ArtifactReference fallbackReference = fallbackArtifactProvider.get(topic, headers, data);
                    return getSchemaResolver().resolveSchemaByArtifactReference(fallbackReference);
                } catch (RuntimeException fe) {
                    fe.addSuppressed(e);
                    throw fe;
                }
            }
        }
    }
}
