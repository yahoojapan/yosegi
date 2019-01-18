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

import java.io.IOException;

public class JsonNodeToParser {

  /**
   * Convert JsonNode to IParser.
   */
  public static IParser get( final JsonNode jsonNode ) throws IOException {
    if ( jsonNode instanceof ObjectNode ) {
      return new JacksonObjectParser( (ObjectNode)jsonNode );
    } else if ( jsonNode instanceof ArrayNode ) {
      return new JacksonArrayParser( (ArrayNode)jsonNode );
    }
    return new JacksonNullParser();
  }

  /**
   * Checks whether the specified JsonNode has child Node.
   */
  public static boolean hasParser( final JsonNode jsonNode ) throws IOException {
    if ( jsonNode instanceof ObjectNode || jsonNode instanceof ArrayNode ) {
      return true;
    }

    return false;
  }

}
