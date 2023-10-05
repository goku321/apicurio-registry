package io.apicurio.registry.serde.data;

import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.resolver.data.Metadata;
import io.apicurio.registry.resolver.strategy.ArtifactReference;

/**
 * NATS specific implementation for the Record Metadata abstraction used by the SchemaResolver
 * @author Deepak Sah
 */
public class NATSSerdeMetadata implements Metadata {

    private String subject;
    private boolean isKey;
    private Headers headers;

    public NATSSerdeMetadata(String subject, boolean isKey, Headers headers) {
        this.subject = subject;
        this.isKey = isKey;
        this.headers = headers;
    }

    /**
     * @see io.apicurio.registry.resolver.data.Metadata#artifactReference()
     */
    @Override
    public ArtifactReference artifactReference() {
        return null;
    }

    /**
     * @return the topic
     */
    public String getSubject() {
        return subject;
    }
    /**
     * @return the isKey
     */
    public boolean isKey() {
        return isKey;
    }

    /**
     * @return the headers
     */
    public Headers getHeaders() {
        return headers;
    }

}
