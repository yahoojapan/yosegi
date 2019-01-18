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
package jp.co.yahoo.yosegi.spread.expression;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.filter.PerfectMatchStringFilter;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.reader.YosegiReader;

public class TestExpression {

  private Spread getTestSpread() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    config.set( "spread.column.maker.setting" , "{ \"column_name\" : \"root\" , \"string_maker_class\" : \"jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker\" }" );
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "spread/expression/TestExpression.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    return reader.next();
  }

  @Test
  public void T_expression_1() throws IOException{
    Spread spread = getTestSpread();

    StringExtractNode col1Node = new StringExtractNode( "col1" );
    StringExtractNode col21Node = new StringExtractNode( "col2-1" );
    StringExtractNode col2Node = new StringExtractNode( "col2" , col21Node );

    IExpressionNode node = new AndExpressionNode();
    node.addChildNode( new ExecuterNode( col1Node , new PerfectMatchStringFilter( "a" ) ) );

    IExpressionIndex result = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    assertEquals( ( result instanceof FilterdExpressionIndex ) , true );
    assertEquals( result.size() , 1 );
    assertEquals( result.get(0) , 0 );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get(0).getRow() ) ).getString() , "a" );
  }

  @Test
  public void T_expression_2() throws IOException{
    Spread spread = getTestSpread();

    StringExtractNode col1Node = new StringExtractNode( "col1" );
    StringExtractNode col21Node = new StringExtractNode( "col2-1" );
    StringExtractNode col2Node = new StringExtractNode( "col2" , col21Node );

    IExpressionNode node = new AndExpressionNode();
    node.addChildNode( new ExecuterNode( col1Node , new PerfectMatchStringFilter( "a" ) ) );
    node.addChildNode( new ExecuterNode( col1Node , new PerfectMatchStringFilter( "b" ) ) );

    IExpressionIndex result = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    assertEquals( ( result instanceof FilterdExpressionIndex ) , true );
    assertEquals( result.size() , 0 );
  }

  @Test
  public void T_expression_3() throws IOException{
    Spread spread = getTestSpread();

    StringExtractNode col1Node = new StringExtractNode( "col1" );
    StringExtractNode col21Node = new StringExtractNode( "col2-1" );
    StringExtractNode col2Node = new StringExtractNode( "col2" , col21Node );

    IExpressionNode node = new OrExpressionNode();
    node.addChildNode( new ExecuterNode( col1Node , new PerfectMatchStringFilter( "a" ) ) );
    node.addChildNode( new ExecuterNode( col1Node , new PerfectMatchStringFilter( "c" ) ) );

    IExpressionIndex result = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    assertEquals( ( result instanceof FilterdExpressionIndex ) , true );
    assertEquals( spread.size() , 5 );
    assertEquals( result.size() , 2 );
    assertEquals( result.get(0) , 0 );
    assertEquals( result.get(1) , 2 );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get( result.get(0) ).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get( result.get(1) ).getRow() ) ).getString() , "c" );
  }

  @Test
  public void T_expression_4() throws IOException{
    Spread spread = getTestSpread();

    StringExtractNode col1Node = new StringExtractNode( "col1" );
    StringExtractNode col21Node = new StringExtractNode( "col2-1" );
    StringExtractNode col2Node = new StringExtractNode( "col2" , col21Node );

    IExpressionNode node = new NotExpressionNode();
    node.addChildNode( new ExecuterNode( col1Node , new PerfectMatchStringFilter( "a" ) ) );

    IExpressionIndex result = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    assertEquals( ( result instanceof FilterdExpressionIndex ) , true );
    assertEquals( result.size() , 4 );
    assertEquals( result.get(0) , 1 );
    assertEquals( result.get(1) , 2 );
    assertEquals( result.get(2) , 3 );
    assertEquals( result.get(3) , 4 );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get( result.get(0) ).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get( result.get(1) ).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get( result.get(2) ).getRow() ) ).getString() , "d" );
    assertEquals( ( (PrimitiveObject)( spread.getColumn("col1").get( result.get(3) ).getRow() ) ).getString() , "e" );
  }

}
