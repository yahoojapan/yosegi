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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import jp.co.yahoo.yosegi.message.objects.ObjectToJsonNode;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObjectToJsonNode;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ObjectDispatchByClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JacksonArrayFormatter implements IJacksonFormatter {
  @FunctionalInterface
  private interface DispatchedFunc {
    void accept(ArrayNode array, Object childObj) throws IOException;
  }

  private static ObjectDispatchByClass.Func<DispatchedFunc> dispatcher;

  static {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.setDefault((array, childObj) -> array.add(ObjectToJsonNode.get(childObj)));
    sw.set(List.class, (array, childObj) ->
        array.add(JacksonContainerToJsonObject.getFromList((List<Object>)childObj)));
    sw.set(Map.class, (array, childObj) ->
        array.add(JacksonContainerToJsonObject.getFromMap((Map<Object,Object>)childObj)));
    dispatcher = sw.create();
  }

  @Override
  public JsonNode write( final Object obj ) throws IOException {
    ArrayNode array = new ArrayNode( JsonNodeFactory.instance );
    if (!(obj instanceof List)) {
      return array;
    }

    List<Object> listObj = (List)obj;
    for ( Object childObj : listObj ) {
      dispatcher.get(childObj).accept(array, childObj);
    }

    return array;
  }

  @Override
  public JsonNode writeParser( final IParser parser ) throws IOException {
    ArrayNode array = new ArrayNode( JsonNodeFactory.instance );
    for ( int i = 0 ; i < parser.size() ; i++ ) {
      IParser childParser = parser.getParser( i );
      if ( childParser.isMap() || childParser.isStruct() ) {
        array.add( JacksonParserToJsonObject.getFromObjectParser( childParser ) );
      } else if ( childParser.isArray() ) {
        array.add( JacksonParserToJsonObject.getFromArrayParser( childParser ) );
      } else {
        array.add( PrimitiveObjectToJsonNode.get( parser.get( i ) ) );
      }
    }
    return array;
  }

}
