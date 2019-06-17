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
import java.util.List;

public class TestTextArrayFormatter {

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
  public void createNewInstanceFromDelimiterSetting() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
  }

  @Test
  public void createNewInstanceFromNotDelimiterSetting() {
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) );
    assertThrows( IOException.class ,
      () -> {
        TextArrayFormatter formatter = new TextArrayFormatter( schema );
      }
    );
  }

  @Test
  public void createNewInstanceFromNull() {
    assertThrows( NullPointerException.class ,
      () -> {
        TextArrayFormatter formatter = new TextArrayFormatter( null );
      }
    );
  }

  @Test
  public void createSimpleArrayText() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , Arrays.asList( "a" , "b" , "c" ) );
    assertEquals( "a,b,c" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void createSingleArrayText() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , Arrays.asList( "a" ) );
    assertEquals( "a" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void createEmptyArrayText() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , Arrays.asList() );
    assertEquals( 0 , buffer.getLength() );
  }

  @Test
  public void createArrayTextFromNotListObject() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , "a" );
    assertEquals( 0 , buffer.getLength() );
  }

  @Test
  public void createArrayTextFromNull() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , null );
    assertEquals( 0 , buffer.getLength() );
  }

  @Test
  public void createArrayTextFromParser() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.writeParser( buffer , null , new TestPrimitiveParser( true ) );
    assertEquals( "a,b,c" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void createArrayTextFromNullParser() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "root" , new StringField( "obj" ) , option );
    TextArrayFormatter formatter = new TextArrayFormatter( schema );
    ByteArrayData buffer = new ByteArrayData();
    formatter.writeParser( buffer , null , new TextNullParser() );
    assertEquals( "" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

}
