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

import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.text.TextNullParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTextMapFormatter {

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
      return new TextNullParser();
    }

    @Override
    public IParser getParser( final int index ) throws IOException {
      return new TextNullParser();
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
  public void T_createNewInstance_void_withDelimiterSetting() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
  }

  @Test
  public void T_createNewInstance_throwsException_withNotDelimiterSetting() {
    Properties option = new Properties();
    option.set( "field_delimiter" , "0x2c" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) );
    assertThrows( IOException.class ,
      () -> {
        TextMapFormatter formatter = new TextMapFormatter( schema );
      }
    );
  }

  @Test
  public void T_createNewInstance_throwsException_withNotFieldDelimiterSetting() {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) );
    assertThrows( IOException.class ,
      () -> {
        TextMapFormatter formatter = new TextMapFormatter( schema );
      }
    );
  }

  @Test
  public void T_createNewInstance_throwsException_withNotDelimiterAndFieldDelimiterSetting() {
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) );
    assertThrows( IOException.class ,
      () -> {
        TextMapFormatter formatter = new TextMapFormatter( schema );
      }
    );
  }

  @Test
  public void T_createNewInstance_throwsException_withNull() {
    assertThrows( NullPointerException.class ,
      () -> {
        TextMapFormatter formatter = new TextMapFormatter( null );
      }
    );
  }

  @Test
  public void T_write_mapText_withSameValueMap() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    Map map = new HashMap<String,Object>();
    map.put( "a" , "1" );
    map.put( "b" , "2" );
    map.put( "c" , "3" );
    formatter.write( buffer , map );
    assertEquals( "a=1,b=2,c=3" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_write_mapText_withSingleValueMap() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    Map map = new HashMap<String,Object>();
    map.put( "a" , "1" );
    formatter.write( buffer , map );
    assertEquals( "a=1" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_write_emptyText_withEmptyMap() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    Map map = new HashMap<String,Object>();
    formatter.write( buffer , map );
    assertEquals( 0 , buffer.getLength() );
  }

  @Test
  public void T_write_emptyText_withNotMap() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , "a=1" );
    assertEquals( 0 , buffer.getLength() );
  }

  @Test
  public void T_write_emptyText_withNull() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , null );
    assertEquals( 0 , buffer.getLength() );
  }

  @Test
  public void T_writeParser_mapText_withParser() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.writeParser( buffer , null , new TestPrimitiveParser( false ) );
    assertEquals( "key1=a,key2=b,key3=c" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_writeParser_emptyText_withNullParser() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    MapContainerField schema = new MapContainerField( "root" , new StringField( "obj" ) , option );
    TextMapFormatter formatter = new TextMapFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.writeParser( buffer , null , new TextNullParser() );
    assertEquals( "" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

}
