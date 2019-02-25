/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.common.serialization;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class SimpleKeyValueDeserializationSchema implements KeyValueDeserializationSchema<JSONObject> , SerializationSchema<String> {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    private transient Charset charset;

    public String keyField;
    public String valueField;

    public SimpleKeyValueDeserializationSchema(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    public SimpleKeyValueDeserializationSchema() {
        this(StandardCharsets.UTF_8);
        this.keyField = DEFAULT_KEY_FIELD;
        this.valueField = DEFAULT_VALUE_FIELD;
    }

    @Override
    public JSONObject deserializeKeyAndValue(byte[] key, byte[] value, JSONObject map) {
        if (keyField != null) {
            String k = key != null ? new String(key, charset) : null;
            map.put(keyField, k);
        }
        if (valueField != null) {
            String v = value != null ? new String(value, charset) : null;
            map.put(valueField, v);
        }
        return map;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }

    @Override
    public byte[] serialize(String element) {
        return element.getBytes(charset);
    }

    private void writeObject (ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(charset.name());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}
