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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestJacksonContainerToJsonObject {

  @Test
  public void T_getFromList_equalsSetValue_withFromList() throws IOException {
    List<Object> list = Arrays.asList( "a" , "b" , "c" );
    JsonNode node = JacksonContainerToJsonObject.getFromList( list );
    assertTrue( ( node instanceof ArrayNode ) );

    ArrayNode arrayNode = (ArrayNode)node;
    assertEquals( arrayNode.size() , 3 );
    assertTrue( arrayNode.get(0).isTextual() );
    assertTrue( arrayNode.get(1).isTextual() );
    assertTrue( arrayNode.get(2).isTextual() );
    assertEquals( arrayNode.get(0).asText() , "a" );
    assertEquals( arrayNode.get(1).asText() , "b" );
    assertEquals( arrayNode.get(2).asText() , "c" );
  }

  @Test
  public void T_getFromList_emptyArrayNode_withNull() throws IOException {
    JacksonArrayFormatter formatter = new JacksonArrayFormatter();
    JsonNode node = JacksonContainerToJsonObject.getFromList( null );
    assertTrue( ( node instanceof ArrayNode ) );

    ArrayNode arrayNode = (ArrayNode)node;
    assertEquals( arrayNode.size() , 0 );
  }


  @Test
  public void T_getFromMap_equalsSetValue_withMap() throws IOException {
    Map<Object,Object> map = new HashMap<Object,Object>();
    map.put( "key1" , "a" );
    map.put( "key2" , "b" );
    map.put( "key3" , "c" );
    JsonNode node = JacksonContainerToJsonObject.getFromMap( map );
    assertTrue( ( node instanceof ObjectNode ) );
    ObjectNode objNode = (ObjectNode)node;

    assertEquals( objNode.size() , 3 );
    assertTrue( objNode.get("key1").isTextual() );
    assertTrue( objNode.get("key2").isTextual() );
    assertTrue( objNode.get("key3").isTextual() );
    assertEquals( objNode.get("key1").asText() , "a" );
    assertEquals( objNode.get("key2").asText() , "b" );
    assertEquals( objNode.get("key3").asText() , "c" );
  }

  @Test
  public void T_getFromMap_emptyMap_withNull() throws IOException {
    JsonNode node = JacksonContainerToJsonObject.getFromMap( null );
    assertTrue( ( node instanceof ObjectNode ) );
    ObjectNode objNode = (ObjectNode)node;

    assertEquals( objNode.size() , 0 );
  }

}
