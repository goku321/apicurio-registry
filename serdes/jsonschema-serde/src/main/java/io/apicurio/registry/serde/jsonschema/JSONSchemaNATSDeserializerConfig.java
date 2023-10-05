package io.apicurio.registry.serde.jsonschema;

import io.apicurio.registry.serde.config.BaseNATSSerDeConfig;
import java.util.Map;
import static io.apicurio.registry.serde.SerdeConfig.*;

public class JSONSchemaNATSDeserializerConfig extends BaseNATSSerDeConfig {
    public static final String SPECIFIC_RETURN_CLASS_DOC =
            "The specific class to use for deserializing the data into java objects";

//    private static ConfigDef configDef() {
//        ConfigDef configDef = new ConfigDef()
//                .define(DESERIALIZER_SPECIFIC_KEY_RETURN_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SPECIFIC_RETURN_CLASS_DOC)
//                .define(DESERIALIZER_SPECIFIC_VALUE_RETURN_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SPECIFIC_RETURN_CLASS_DOC)
//                .define(VALIDATION_ENABLED, ConfigDef.Type.BOOLEAN, VALIDATION_ENABLED_DEFAULT, ConfigDef.Importance.MEDIUM, "Whether to validate the data against the json schema");
//        return configDef;
//    }

//    private boolean isKey;
    /**
     * Constructor.
     * @param originals
     */
    public JSONSchemaNATSDeserializerConfig(Map<?, ?> originals) {
        super(originals);
    }

    public Class<?> getSpecificReturnClass() {
        return this.getClass(DESERIALIZER_SPECIFIC_KEY_RETURN_CLASS);
    }

    public Class<?> getClass(String key) {
        return (Class)this.get(key);
    }

//    public boolean validationEnabled() {
//        return this.getBoolean(VALIDATION_ENABLED);
//    }
}
