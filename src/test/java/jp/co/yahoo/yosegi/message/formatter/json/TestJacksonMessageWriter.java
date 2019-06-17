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

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonNullParser;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

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

public class TestJacksonMessageWriter {

  private class TestPrimitiveParser implements IParser {

    private final boolean isArray;

    public TestPrimitiveParser( final boolean isArray ) {
      this.isArray = isArray;
    }

    @Override
    public PrimitiveObject get(final String key ) throws IOException {
      if ( "key1".equals( key ) ) {
        return new StringObj( "a" );
      } else if ( "key2".equals( key ) ) {
        return new StringObj( "b" );
      } else if ( "key3".equals( key ) ) {
        return new StringObj( "c" );
      }
      return null;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException {
      if ( index == 0 ) {
        return new StringObj( "a" );
      } else if ( index == 1 ) {
        return new StringObj( "b" );
      } else if ( index == 2 ) {
        return new StringObj( "c" );
      }
      return null;
    }

    @Override
    public IParser getParser( final String key ) throws IOException {
      return new JacksonNullParser();
    }

    @Override
    public IParser getParser( final int index ) throws IOException {
      return new JacksonNullParser();
    }

    @Override
    public String[] getAllKey() throws IOException {
      return new String[]{ "key1" , "key2" , "key3" };
    }

    @Override
    public boolean containsKey( final String key ) throws IOException {
      if ( "key1".equals( key ) ) {
        return true;
      } else if ( "key2".equals( key ) ) {
        return true;
      } else if ( "key3".equals( key ) ) {
        return true;
      }
      return false;
    }

    @Override
    public int size() throws IOException {
      return 3;
    }

    @Override
    public boolean isArray() throws IOException {
      return isArray;
    }

    @Override
    public boolean isMap() throws IOException {
      return ! isArray;
    }

    @Override
    public boolean isStruct() throws IOException {
      return ! isArray;
    }

    @Override
    public boolean hasParser( final int index ) throws IOException {
      return false;
    }

    @Override
    public boolean hasParser( final String key ) throws IOException {
      return false;
    }

    @Override
    public Object toJavaObject() throws IOException {
      return null;
    }

  }

  @Test
  public void createJsonStringFromPrimitiveObject() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    byte[] result = writer.create( new StringObj( "a" ) );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof TextNode ) );
    assertEquals( node.asText() , "a" );
  }

  @Test
  public void createJsonStringFromNullPrimitiveObject() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    PrimitiveObject obj = null;
    byte[] result = writer.create( obj );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof NullNode ) );
  }

  @Test
  public void createJsonStringFromList() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    List<Object> list = Arrays.asList( "a" , "b" , "c" );
    byte[] result = writer.create( list );
    JsonNode node = mapper.readTree( result );
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
  public void createJsonStringFromNullList() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    List<Object> list = null;
    byte[] result = writer.create( list );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof ArrayNode ) );

    ArrayNode arrayNode = (ArrayNode)node;
    assertEquals( arrayNode.size() , 0 );
  }

  @Test
  public void createJsonStringFromMap() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    Map<Object,Object> map = new HashMap<Object,Object>();
    map.put( "key1" , "a" );
    map.put( "key2" , "b" );
    map.put( "key3" , "c" );
    byte[] result = writer.create( map );
    JsonNode node = mapper.readTree( result );
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
  public void createJsonStringFromNullMap() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    Map<Object,Object> map = null;
    byte[] result = writer.create( map );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode objNode = (ObjectNode)node;
    assertEquals( objNode.size() , 0 );
  }

  @Test
  public void createJsonStringFromObjectParser() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    byte[] result = writer.create( new TestPrimitiveParser( false ) );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode arrayNode = (ObjectNode)node;
    assertEquals( arrayNode.size() , 3 );
    assertTrue( arrayNode.get("key1").isTextual() );
    assertTrue( arrayNode.get("key2").isTextual() );
    assertTrue( arrayNode.get("key3").isTextual() );
    assertEquals( arrayNode.get("key1").asText() , "a" );
    assertEquals( arrayNode.get("key2").asText() , "b" );
    assertEquals( arrayNode.get("key3").asText() , "c" );
  }

  @Test
  public void createJsonStringFromNullParserVar() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    IParser parser = null;
    byte[] result = writer.create( parser );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof NullNode ) );
  }

  @Test
  public void createJsonStringFromArrayParser() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    byte[] result = writer.create( new TestPrimitiveParser( true ) );
    JsonNode node = mapper.readTree( result );
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
  public void createJsonStringFromNullParser() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JacksonMessageWriter writer = new JacksonMessageWriter();
    byte[] result = writer.create( new JacksonNullParser() );
    JsonNode node = mapper.readTree( result );
    assertTrue( ( node instanceof NullNode ) );
  }

}
