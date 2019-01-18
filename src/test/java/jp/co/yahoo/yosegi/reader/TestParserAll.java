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
package jp.co.yahoo.yosegi.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.ExecuterNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.StringExtractNode;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.writer.YosegiSchemaStreamWriter;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

public class TestParserAll{

  private InputStream readFile() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    YosegiSchemaStreamWriter writer = new YosegiSchemaStreamWriter( out , new Configuration() );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "parser/TestParserAll.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.write( parser );
      line = in.readLine();
    }
    writer.close();

    return new ByteArrayInputStream( out.toByteArray() );
  }

  private InputStream createFile() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    YosegiWriter writer = new YosegiWriter( out , new Configuration() );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "parser/TestParserAll.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      Spread spread = new Spread();
      spread.addParserRow( parser );
      writer.append( spread );
      line = in.readLine();
    }
    writer.close();

    return new ByteArrayInputStream( out.toByteArray() );
  }

  @Test
  public void T_parse_1() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject col1 = parser.get( "col1" );
      assertEquals( "string" , col1.getString() );
    }
    reader.close();
  }

  @Test
  public void T_parser_2() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    int i = 0;
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser col3 = parser.getParser( "col3" );
      PrimitiveObject a = col3.get( "a" );
      if( i == 0 ){
        assertEquals( a.getBoolean() , true );
      }
      else if( i == 3 ){
        assertEquals( a.getBoolean() , false );
      }
      else{
        assertEquals( a , null );
      }
      i++;
    }
    reader.close();
  }

  @Test
  public void T_parser_3() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser col2 = parser.getParser( "col2" );
      for( int i = 0 ; i < col2.size() ; i++ ){
        PrimitiveObject a = col2.get(i);
        assertEquals( a.getInt() , i );
      }
    }
    reader.close();
  }

  @Test
  public void T_parser_4() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject a = parser.get( "col5" );
      assertEquals( a , null );
    }
    reader.close();
  }

  @Test
  public void T_parser_5() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser parser2 = parser.getParser( "col5" );
      PrimitiveObject a = parser2.get( "a" );
      assertEquals( a , null );
    }
    reader.close();
  }

  @Test
  public void T_parser_6() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject a = parser.get( "col3" );
      assertEquals( a , null );
    }
    reader.close();
  }

  @Test
  public void T_parser_7() throws IOException{
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( createFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
    }
    reader.close();
  }

}
