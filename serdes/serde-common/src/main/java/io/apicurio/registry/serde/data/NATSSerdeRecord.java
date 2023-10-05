package io.apicurio.registry.serde.data;

import io.apicurio.registry.resolver.data.Record;

public class NATSSerdeRecord<T> implements Record<T> {

    private NATSSerdeMetadata metadata;
    private T payload;

    public NATSSerdeRecord(NATSSerdeMetadata metadata, T payload) {
        this.metadata = metadata;
        this.payload = payload;
    }

    /**
     * @see io.apicurio.registry.resolver.data.Record#metadata()
     */
    @Override
    public NATSSerdeMetadata metadata() {
        return metadata;
    }

    /**
     * @see io.apicurio.registry.resolver.data.Record#payload()
     */
    @Override
    public T payload() {
        return payload;
    }

}
