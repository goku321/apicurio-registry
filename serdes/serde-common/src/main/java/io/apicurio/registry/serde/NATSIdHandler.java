package io.apicurio.registry.serde;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.serde.config.IdOption;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class NATSIdHandler implements IdHandler {
    static final int idSize = 8; // we use 8 / long

    private IdOption idOption = IdOption.globalId;

    @Override
    public void configure(Map<String, Object> configs, boolean isKey) {

    }

    @Override
    public void writeId(ArtifactReference reference, OutputStream out) throws IOException {
        long id;
        id = reference.getGlobalId();
        out.write(ByteBuffer.allocate(idSize).putLong(id).array());
    }

    @Override
    public void writeId(ArtifactReference reference, ByteBuffer buffer) {
        long id;
        id = reference.getGlobalId();
        buffer.putLong(id);
    }

    @Override
    public ArtifactReference readId(ByteBuffer buffer) {
        return ArtifactReference.builder().globalId(buffer.getLong()).build();
    }

    @Override
    public int idSize() {
        return idSize;
    }
}
