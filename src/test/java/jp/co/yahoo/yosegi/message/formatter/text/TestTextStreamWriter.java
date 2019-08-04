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

package jp.co.yahoo.yosegi.message.formatter.text;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.text.TextNullParser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestTextStreamWriter {

  private class TestPrimitiveParser implements IParser {

    @Override
    public PrimitiveObject get(final String key ) throws IOException {
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
      return new TextNullParser();
    }

    @Override
    public IParser getParser( final int index ) throws IOException {
      return new TextNullParser();
    }

    @Override
    public String[] getAllKey() throws IOException {
      return new String[]{};
    }

    @Override
    public boolean containsKey( final String key ) throws IOException {
      return false;
    }

    @Override
    public int size() throws IOException {
      return 3;
    }

    @Override
    public boolean isArray() throws IOException {
      return true;
    }

    @Override
    public boolean isMap() throws IOException {
      return false;
    }

    @Override
    public boolean isStruct() throws IOException {
      return false;
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
  public void T_write_text_withPrimitiveType() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    StringField schema = new StringField( "field" );
    TextStreamWriter writer = new TextStreamWriter( out , schema );
    writer.write( new StringObj( "value1" ) );
    writer.write( new StringObj( "value2" ) );
    writer.close();
    String result = new String( out.toByteArray() );
    assertEquals( "value1\nvalue2\n" , result );
  }

  @Test
  public void T_write_text_withList() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextStreamWriter writer = new TextStreamWriter( out , schema );

    writer.write( Arrays.asList( "value1" , "value2" ) );
    writer.write( Arrays.asList( "value3" , "value4" ) );
    writer.close();
    String result = new String( out.toByteArray() );
    assertEquals( "value1,value2\nvalue3,value4\n" , result );
  }

  @Test
  public void T_write_text_withMap() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    StructContainerField schema = new StructContainerField( "root" , option );
    schema.set( new StringField( "key1" ) );
    schema.set( new StringField( "key2" ) );
    TextStreamWriter writer = new TextStreamWriter( out , schema );

    Map<Object,Object> map = new HashMap<Object,Object>();
    map.put( "key1" , "value1-1" );
    map.put( "key2" , "value2-1" );
    writer.write( map );
    map.clear();
    map.put( "key1" , "value1-2" );
    map.put( "key2" , "value2-2" );
    writer.write( map );
    writer.close();
    String result = new String( out.toByteArray() );
    assertEquals( "value1-1,value2-1\nvalue1-2,value2-2\n" , result );
  }

  @Test
  public void T_write_text_withParser() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextStreamWriter writer = new TextStreamWriter( out , schema );

    writer.write( new TestPrimitiveParser() );
    writer.write( new TestPrimitiveParser() );
    String result = new String( out.toByteArray() );
    assertEquals( "a,b,c\na,b,c\n" , result );
  }

}
