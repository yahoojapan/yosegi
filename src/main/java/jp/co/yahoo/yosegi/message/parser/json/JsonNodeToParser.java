/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.parser.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;
import java.util.function.Function;

public class JsonNodeToParser {
  @FunctionalInterface
  private interface GetDispatchedFunc {
    public IParser apply(JsonNode jsonNode) throws IOException;
  }

  private static SwitchDispatcherFactory.Func<Class, GetDispatchedFunc> getDispatcher;
  private static SwitchDispatcherFactory.Func<Class, Boolean> hasParserDispatcher;

  static {
    SwitchDispatcherFactory<Class, GetDispatchedFunc> sw = new SwitchDispatcherFactory<>();
    sw.setDefault(jsonNode -> new JacksonNullParser());
    sw.set(ObjectNode.class, jsonNode -> new JacksonObjectParser((ObjectNode)jsonNode));
    sw.set(ArrayNode.class,  jsonNode -> new JacksonArrayParser((ArrayNode)jsonNode));
    getDispatcher = sw.create();

    hasParserDispatcher = (new SwitchDispatcherFactory<>())
        .setDefault(false)
        .set(ObjectNode.class, true)
        .set(ArrayNode.class,  true)
        .create();
  }

  /**
   * Convert JsonNode to IParser.
   */
  public static IParser get(final JsonNode jsonNode) throws IOException {
    return getDispatcher.get(jsonNode.getClass()).apply(jsonNode);
  }

  /**
   * Checks whether the specified JsonNode has child Node.
   */
  public static boolean hasParser(final JsonNode jsonNode) throws IOException {
    return hasParserDispatcher.get(jsonNode.getClass());
  }
}

