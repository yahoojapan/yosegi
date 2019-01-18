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
package jp.co.yahoo.yosegi.message.parser.text;

import java.io.IOException;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.design.StringField;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.parser.IMessageReader;
import jp.co.yahoo.yosegi.message.parser.IParser;

public class TestTextMessageReader {

  @Test
  public void T_example_array() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "array" , new LongField( "array_value" ) , properties );

    IMessageReader reader = new TextMessageReader( schema );

    byte[] dummyData = " 100,200,300 ".getBytes("UTF-8");

    IParser parser = reader.create( dummyData , 1 , dummyData.length - 2 );
    System.out.println( parser.get(1).getString() );
    System.out.println( parser.get(2).getString() );
    System.out.println( parser.get(0).getString() );
    System.out.println( parser.get(3) );

    for( int i = 0 ; i < parser.size() ; i++ ){
      System.out.println( parser.get(i).getString() );
    }
  }

  @Test
  public void T_example_struct() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x2c" );
    StructContainerField schema = new StructContainerField( "" , properties );
    schema.set( new StringField( "f1" ) );
    schema.set( new StringField( "f2" ) );
    schema.set( new StringField( "f3" ) );
    schema.set( new StringField( "f4" ) );
    schema.set( new StringField( "f5" ) );

    IMessageReader reader = new TextMessageReader( schema );

    byte[] dummyData = " 100,200,300 ".getBytes("UTF-8");

    IParser parser = reader.create( dummyData , 1 , dummyData.length - 2 );

    String[] keys = parser.getAllKey();
    for( int i = 0 ; i < keys.length ; i++ ){
      System.out.println( parser.get( keys[i] ).getString() );
    }
    System.out.println( parser.get( "f6" ) );
  }

  @Test
  public void T_example_map() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x2c" );
    properties.set( "field_delimiter" , "0x3D" );
    MapContainerField schema = new MapContainerField( "" , new StringField( "" ) , properties );

    IMessageReader reader = new TextMessageReader( schema );

    byte[] dummyData = "f1=100,f2=200,f3=300,f4=2,f5=2".getBytes("UTF-8");

    IParser parser = reader.create( dummyData , 0 , dummyData.length );

    System.out.println( parser.get( "f3" ).getString() );
    System.out.println( parser.get( "f2" ).getLong() );
    //System.out.println( parser.get( "f5" ) );

    String[] keys = parser.getAllKey();
    for( int i = 0 ; i < keys.length ; i++ ){
      System.out.println( parser.get( keys[i] ).getString() );
    }
    System.out.println( parser.get( "f6" ) );
  }

  @Test
  public void T_example_nest() throws IOException{
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x20" );
    StructContainerField schema = new StructContainerField( "" , properties );

    Properties arrayProperties = new Properties();
    arrayProperties.set( "delimiter" , "0x2c" );
    schema.set( new ArrayContainerField( "array" , new LongField( "array_value" ) , arrayProperties ) );

    Properties mapProperties = new Properties();
    mapProperties.set( "delimiter" , "0x2c" );
    mapProperties.set( "field_delimiter" , "0x3D" );
    schema.set( new MapContainerField( "map" , new StringField( "" ) , mapProperties ) );

    IMessageReader reader = new TextMessageReader( schema );

    byte[] dummyData = " 100,200,300 f1=b,f2=hogehoge,f3=,f4=5  ".getBytes("UTF-8");

    IParser parser = reader.create( dummyData , 1 , dummyData.length - 3 );
    for( String key : parser.getAllKey() ){
      System.out.println( key );
      System.out.println( parser.get(key).getString() );
    }

    IParser arrayParser = parser.getParser( "array" );
    for( int i = 0 ; i < arrayParser.size() ; i++ ){
      System.out.println( arrayParser.get( i ).getString() );
    }

    IParser mapParser = parser.getParser( "map" );
    for( String mapKey : mapParser.getAllKey() ){
      System.out.println( mapKey );
      System.out.println( mapParser.get( mapKey ).getString() );
    }
  }

}
