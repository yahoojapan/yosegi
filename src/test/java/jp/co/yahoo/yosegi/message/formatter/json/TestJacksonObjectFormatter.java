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

public class TestJacksonObjectFormatter {

  private class TestParentParser implements IParser {

    private final boolean isArray;

    public TestParentParser( final boolean isArray ) {
      this.isArray = isArray;
    }

    @Override
    public PrimitiveObject get(final String key ) throws IOException {
      return null;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException {
      return null;
    }

    @Override
    public IParser getParser( final String key ) throws IOException {
      if ( "key1".equals( key ) ) {
        return new TestPrimitiveParser( isArray );
      }
      return new JacksonNullParser();
    }

    @Override
    public IParser getParser( final int index ) throws IOException {
      return new JacksonNullParser();
    }

    @Override
    public String[] getAllKey() throws IOException {
      return new String[]{ "key1" };
    }

    @Override
    public boolean containsKey( final String key ) throws IOException {
      return "key1".equals( key );
    }

    @Override
    public int size() throws IOException {
      return 0;
    }

    @Override
    public boolean isArray() throws IOException {
      return false;
    }

    @Override
    public boolean isMap() throws IOException {
      return true;
    }

    @Override
    public boolean isStruct() throws IOException {
      return true;
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
  public void T_write_equalsSetValue_withMap() throws IOException {
    Map<Object,Object> map = new HashMap<Object,Object>();
    map.put( "key1" , "a" );
    map.put( "key2" , "b" );
    map.put( "key3" , "c" );
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.write( map );
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
  public void T_write_equalsSetValue_withNull() throws IOException {
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.write( null );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode objNode = (ObjectNode)node;
    assertEquals( objNode.size() , 0 );
  }

  @Test
  public void T_write_equalsSetValue_withNestingArrayNodeFromMap() throws IOException {
    Map<Object,Object> map = new HashMap<Object,Object>();
    map.put( "key1" , Arrays.asList( "a" , "b" , "c" ) );
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.write( map );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode parentNode = (ObjectNode)node;
    assertEquals( parentNode.size() , 1 );

    JsonNode childNode = parentNode.get("key1");
    assertTrue( ( childNode instanceof ArrayNode ) );
    ArrayNode arrayNode = (ArrayNode)childNode;

    assertEquals( arrayNode.size() , 3 );
    assertTrue( arrayNode.get(0).isTextual() );
    assertTrue( arrayNode.get(1).isTextual() );
    assertTrue( arrayNode.get(2).isTextual() );
    assertEquals( arrayNode.get(0).asText() , "a" );
    assertEquals( arrayNode.get(1).asText() , "b" );
    assertEquals( arrayNode.get(2).asText() , "c" );
  }

  @Test
  public void T_write_equalsSetValue_withNestingObjectNodeFromMap() throws IOException {
    Map<Object,Object> child = new HashMap<Object,Object>();
    child.put( "key1" , "a" );
    child.put( "key2" , "b" );
    child.put( "key3" , "c" );
    Map<Object,Object> map = new HashMap<Object,Object>();
    map.put( "key1" , child );
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.write( map );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode parentNode = (ObjectNode)node;
    assertEquals( parentNode.size() , 1 );

    JsonNode childNode = parentNode.get("key1");
    assertTrue( ( childNode instanceof ObjectNode ) );
    ObjectNode objNode = (ObjectNode)childNode;

    assertEquals( objNode.size() , 3 );
    assertTrue( objNode.get("key1").isTextual() );
    assertTrue( objNode.get("key2").isTextual() );
    assertTrue( objNode.get("key3").isTextual() );
    assertEquals( objNode.get("key1").asText() , "a" );
    assertEquals( objNode.get("key2").asText() , "b" );
    assertEquals( objNode.get("key3").asText() , "c" );
  }

  @Test
  public void T_writeParser_equalsSetValue_withParser() throws IOException {
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.writeParser( new TestPrimitiveParser( false ) );
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
  public void T_writeParser_equalsSetValue_withNestingArrayNodeFromParser() throws IOException {
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.writeParser( new TestParentParser( true ) );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode parentNode = (ObjectNode)node;
    assertEquals( parentNode.size() , 1 );

    JsonNode childNode = parentNode.get("key1");
    assertTrue( ( childNode instanceof ArrayNode ) );
    ArrayNode arrayNode = (ArrayNode)childNode;

    assertEquals( arrayNode.size() , 3 );
    assertTrue( arrayNode.get(0).isTextual() );
    assertTrue( arrayNode.get(1).isTextual() );
    assertTrue( arrayNode.get(2).isTextual() );
    assertEquals( arrayNode.get(0).asText() , "a" );
    assertEquals( arrayNode.get(1).asText() , "b" );
    assertEquals( arrayNode.get(2).asText() , "c" );
  }

  @Test
  public void T_writeParser_equalsSetValue_withNestingObjectNodeFromParser() throws IOException {
    JacksonObjectFormatter formatter = new JacksonObjectFormatter();
    JsonNode node = formatter.writeParser( new TestParentParser( false ) );
    assertTrue( ( node instanceof ObjectNode ) );

    ObjectNode parentNode = (ObjectNode)node;
    assertEquals( parentNode.size() , 1 );

    JsonNode childNode = parentNode.get("key1");
    assertTrue( ( childNode instanceof ObjectNode ) );
    ObjectNode objNode = (ObjectNode)childNode;

    assertEquals( objNode.size() , 3 );
    assertTrue( objNode.get("key1").isTextual() );
    assertTrue( objNode.get("key2").isTextual() );
    assertTrue( objNode.get("key3").isTextual() );
    assertEquals( objNode.get("key1").asText() , "a" );
    assertEquals( objNode.get("key2").asText() , "b" );
    assertEquals( objNode.get("key3").asText() , "c" );
  }

}
