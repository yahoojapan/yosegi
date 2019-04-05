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

package jp.co.yahoo.yosegi.message.formatter.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import jp.co.yahoo.yosegi.message.objects.ObjectToJsonNode;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObjectToJsonNode;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JacksonObjectFormatter extends JacksonBaseFormatter implements IJacksonFormatter {
  @Override
  public JsonNode write(final Object obj) throws IOException {
    ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);
    if (!Map.class.isInstance(obj)) {
      return objectNode;
    }

    Map<Object,Object> mapObj = (Map<Object,Object>)obj;
    for (Map.Entry<Object,Object> entry : mapObj.entrySet()) {
      String key = entry.getKey().toString();
      objectNode.put(key, writeDispatcher.get(key.getClass()).apply(entry.getValue()));
    }
    return objectNode;
  }

  @Override
  public JsonNode writeParser( final IParser parser ) throws IOException {
    ObjectNode objectNode = new ObjectNode( JsonNodeFactory.instance );
    for ( String key : parser.getAllKey() ) {
      IParser childParser = parser.getParser( key );
      if ( childParser.isMap() || childParser.isStruct() ) {
        objectNode.put( key , JacksonParserToJsonObject.getFromObjectParser( childParser ) );
      } else if ( childParser.isArray() ) {
        objectNode.put( key , JacksonParserToJsonObject.getFromArrayParser( childParser ) );
      } else {
        objectNode.put( key , PrimitiveObjectToJsonNode.get( parser.get( key ) ) );
      }
    }
    return objectNode;
  }
}

