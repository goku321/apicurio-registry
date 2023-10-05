package io.apicurio.registry.serde.config;

/*
 * Copyright 2021 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static io.apicurio.registry.serde.SerdeConfig.*;

import io.apicurio.registry.serde.NATSIdHandler;
import java.util.HashMap;
import java.util.Map;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

/**
 * @author Fabian Martinez
 */
public class BaseNATSSerDeConfig {
    private final Map<String, String> originals;
    private final Set<String> used;
    private final Map<String, Object> values;

    public void ignore(String key) {
        this.used.add(key);
    }

    public BaseNATSSerDeConfig(Map<?, ?> originals) {
        this.originals = new HashMap<String, String>();
        for (Map.Entry<?, ?> entry : originals.entrySet()) {
            this.originals.put((String) entry.getKey(), (String) entry.getValue());
        }
        this.used = ConcurrentHashMap.newKeySet();
        this.values = new HashMap<>();
        this.values.put(NATSIdHandler.class.getName(), new NATSIdHandler());
    }

    public Map<String, Object> originals() {
        Map<String, Object> copy = new BaseNATSSerDeConfig.RecordingMap();
        copy.putAll(this.originals);
        return copy;
    }

    private class RecordingMap<V> extends HashMap<String, V> {
        private final String prefix;
        private final boolean withIgnoreFallback;

        RecordingMap() {
            this("", false);
        }

        RecordingMap(String prefix, boolean withIgnoreFallback) {
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        RecordingMap(Map<String, ? extends V> m) {
            this(m, "", false);
        }

        RecordingMap(Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
            super(m);
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        public V get(Object key) {
            if (key instanceof String) {
                String stringKey = (String)key;
                String keyWithPrefix;
                if (this.prefix.isEmpty()) {
                    keyWithPrefix = stringKey;
                } else {
                    keyWithPrefix = this.prefix + stringKey;
                }

                BaseNATSSerDeConfig.this.ignore(keyWithPrefix);
                if (this.withIgnoreFallback) {
                    BaseNATSSerDeConfig.this.ignore(stringKey);
                }
            }

            return super.get(key);
        }
    }

//    private static ConfigDef buildConfigDef(ConfigDef base) {
//        ConfigDef configDef = new ConfigDef(base)
//                .define(ID_HANDLER, Type.CLASS, ID_HANDLER_DEFAULT, Importance.MEDIUM, "TODO docs")
//                .define(ENABLE_CONFLUENT_ID_HANDLER, Type.BOOLEAN, false, Importance.LOW, "TODO docs")
//                .define(ENABLE_HEADERS, Type.BOOLEAN, ENABLE_HEADERS_DEFAULT, Importance.MEDIUM, "TODO docs")
//                .define(HEADERS_HANDLER, Type.CLASS, HEADERS_HANDLER_DEFAULT, Importance.MEDIUM, "TODO docs")
//                .define(USE_ID, Type.STRING, USE_ID_DEFAULT, Importance.MEDIUM, "TODO docs");
//        return configDef;
//    }
//
//    public BaseKafkaSerDeConfig(ConfigDef configDef, Map<?, ?> originals) {
//        super(buildConfigDef(configDef), originals, false);
//    }
//
//    public BaseKafkaSerDeConfig(Map<?, ?> originals) {
//        super(buildConfigDef(new ConfigDef()), originals, false);
//    }

    public Object getNATSIdHandler() {
        return this.get(NATS_ID_HANDLER);
    }

//    public boolean enableHeaders() {
//        return this.getBoolean(true);
//    }

//    public Object getHeadersHandler() {
//        return this.get(HEADERS_HANDLER);
//    }

    public IdOption useIdOption() {
        return IdOption.valueOf(this.getString(USE_ID));
    }

    public String getString(String key) {
        return (String)this.get(key);
    }

    protected Object get(String key) {
        if (!this.values.containsKey(key)) {
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        } else {
            this.used.add(key);
            return this.values.get(key);
        }
    }

}
