/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.test.dsl.builder.step;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dingodb.test.dsl.builder.checker.SqlChecker;
import io.dingodb.test.dsl.builder.checker.SqlResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlUpdateCountChecker;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

public class StepDeserializer extends StdDeserializer<Step> {
    private static final long serialVersionUID = 5914785535964813231L;

    protected StepDeserializer() {
        super(Step.class);
    }

    @Override
    public Step deserialize(
        @NonNull JsonParser parser,
        DeserializationContext context
    ) throws IOException {
        JsonNode jsonNode = parser.readValueAsTree();
        if (jsonNode.isTextual()) {
            return new SqlStringStep(jsonNode.asText());
        } else if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            SqlChecker checker = null;
            if (objectNode.has("result")) {
                checker = parser.getCodec().treeToValue(
                    objectNode.get("result"),
                    SqlResultChecker.class
                );
            } else if (objectNode.has("count")) {
                Integer updateCount = parser.getCodec().treeToValue(
                    objectNode.get("count"),
                    Integer.class
                );
                checker = new SqlUpdateCountChecker(updateCount);
            }
            if (objectNode.has("sql")) {
                return new SqlStringStep(objectNode.get("sql").asText(), checker);
            } else if (objectNode.has("file")) {
                return new SqlFileNameStep(objectNode.get("file").asText(), checker);
            }
        }
        return (SqlStep) context.handleUnexpectedToken(_valueClass, parser);
    }
}
