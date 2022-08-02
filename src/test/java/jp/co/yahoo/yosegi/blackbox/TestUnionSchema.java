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
package jp.co.yahoo.yosegi.blackbox;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.stream.Stream;

import jp.co.yahoo.yosegi.inmemory.SpreadRawConverter;
import jp.co.yahoo.yosegi.reader.WrapReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.filter.PerfectMatchStringFilter;
import jp.co.yahoo.yosegi.spread.expression.*;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestUnionSchema{

  @Test
  public void T_tmp() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestParserInput.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      System.out.println( spread.toString() );
    }

  }

  @Test
  public void T_1() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn unionColumn = spread.getColumn( "union" );
      assertEquals( unionColumn.getColumnType() , ColumnType.UNION );
    }

  }

  @Test
  public void T_2() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.expand.column" , "{ \"base\" : { \"node\" : \"union\" , \"link_name\" : \"array_from_union\" } }" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn unionColumn = spread.getColumn( "array_from_union" );
      IColumn key1Column = unionColumn.getColumn( "key1" );
      assertEquals( unionColumn.getColumnType() , ColumnType.SPREAD );
      assertEquals( 4 , spread.size() );

      assertEquals( "1" , ( (PrimitiveObject)( key1Column.get(0).getRow() ) ).getString() );
      assertEquals( "hogehoge" , ( (PrimitiveObject)( key1Column.get(1).getRow() ) ).getString() );
      assertEquals( null , key1Column.get(2).getRow() );
      assertEquals( null , key1Column.get(3).getRow() );
    }
  }

  @Test
  public void T_3() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.expand.column" , "{ \"base\" : { \"node\" : \"union\" , \"link_name\" : \"array_from_union\" , \"child_node\" : { \"node\" : \"union_in_array\"  , \"link_name\" : \"union_in_array\" } } } }" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn arrayColumn = spread.getColumn( "union_in_array" );
      assertEquals( arrayColumn.getColumnType() , ColumnType.INTEGER );
      assertEquals( 4 , arrayColumn.size() );

      assertEquals( 1 , ( (PrimitiveObject)( arrayColumn.get(0).getRow() ) ).getInt() );
      assertEquals( 2 , ( (PrimitiveObject)( arrayColumn.get(1).getRow() ) ).getInt() );
      assertEquals( 3 , ( (PrimitiveObject)( arrayColumn.get(2).getRow() ) ).getInt() );
      assertEquals( 4 , ( (PrimitiveObject)( arrayColumn.get(3).getRow() ) ).getInt() );
    }
  }

  @Test
  public void T_4() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.expand.column" , "{ \"base\" : { \"node\" : \"union\" , \"child_node\" : { \"node\" : \"array\"  , \"link_name\" : \"array\"  } } , \"parallel\" : [ { \"link_name\" : \"parallel\" , \"base_link_name\" : \"array\" , \"nodes\" : [ \"union\" , \"array_parallel\" ] } ] }" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn arrayColumn = spread.getColumn( "parallel" );
      assertEquals( arrayColumn.getColumnType() , ColumnType.BOOLEAN );
      assertEquals( 4 , arrayColumn.size() );

      assertEquals( true , ( (PrimitiveObject)( arrayColumn.get(0).getRow() ) ).getBoolean() );
      assertEquals( false , ( (PrimitiveObject)( arrayColumn.get(1).getRow() ) ).getBoolean() );
      assertEquals( true , ( (PrimitiveObject)( arrayColumn.get(2).getRow() ) ).getBoolean() );
      assertEquals( false , ( (PrimitiveObject)( arrayColumn.get(3).getRow() ) ).getBoolean() );
    }
  }

  @Test
  public void T_5() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.expand.column" , "{ \"base\" : { \"node\" : \"union\" , \"child_node\" : { \"node\" : \"array\"  , \"link_name\" : \"array\"  } } , \"parallel\" : [ { \"link_name\" : \"parallel\" , \"base_link_name\" : \"array\" , \"nodes\" : [ \"union\" , \"array_parallel\" ] } ] }" );
    readerConfig.set( "spread.reader.flatten.column" , "[ { \"link_name\" : \"k1\" , \"nodes\" : [\"array\" , \"key1\"] } ]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn arrayColumn = spread.getColumn( "k1" );
      assertEquals( arrayColumn.getColumnType() , ColumnType.STRING );
      assertEquals( 4 , arrayColumn.size() );

      assertEquals( "1" , ( (PrimitiveObject)( arrayColumn.get(0).getRow() ) ).getString() );
      assertEquals( "1" , ( (PrimitiveObject)( arrayColumn.get(1).getRow() ) ).getString() );
      assertEquals( "1" , ( (PrimitiveObject)( arrayColumn.get(2).getRow() ) ).getString() );
      assertEquals( "1" , ( (PrimitiveObject)( arrayColumn.get(3).getRow() ) ).getString() );
    }
  }

  @Test
  public void T_6() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.flatten.column" , "[ { \"link_name\" : \"other\" , \"nodes\" : [\"union\" , \"union_other\"] } ]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn arrayColumn = spread.getColumn( "other" );
      assertEquals( arrayColumn.getColumnType() , ColumnType.UNION );
      assertEquals( 8 , arrayColumn.size() );

      assertEquals( "a" , ( (PrimitiveObject)( arrayColumn.get(0).getRow() ) ).getString() );
      assertEquals( 2 , ( (PrimitiveObject)( arrayColumn.get(1).getRow() ) ).getLong() );
      assertEquals( null , ( (PrimitiveObject)( arrayColumn.get(2).getRow() ) ) );
      assertEquals( null , ( (PrimitiveObject)( arrayColumn.get(3).getRow() ) ) );
      assertEquals( null , ( (PrimitiveObject)( arrayColumn.get(4).getRow() ) ) );
      assertEquals( null , ( (PrimitiveObject)( arrayColumn.get(5).getRow() ) ) );
      assertEquals( null , ( (PrimitiveObject)( arrayColumn.get(6).getRow() ) ) );
      assertEquals( null , ( (PrimitiveObject)( arrayColumn.get(7).getRow() ) ) );
    }
  }

  @Test
  public void T_7() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestUnionSchema.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.flatten.column" , "[ { \"link_name\" : \"other\" , \"nodes\" : [\"union\" , \"union_other\"] } ]" );

    IExpressionNode node = new AndExpressionNode();
    node.addChildNode( new ExecuterNode( new StringExtractNode( "other" ) , new PerfectMatchStringFilter( "a" ) ) );

    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn arrayColumn = spread.getColumn( "other" );

      assertEquals( "a" , ( (PrimitiveObject)( arrayColumn.get( 0 ).getRow() ) ).getString() );
    }
  }

  @Test
  public void T_9() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new ShortObj( (short)1 ) );
    s.addRow( d );
    d.put( "d" , new ByteObj( (byte)2 ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.SHORT );
    }
  }

  @Test
  public void T_10() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new ShortObj( (short)1 ) );
    s.addRow( d );
    d.put( "d" , new ByteObj( (byte)2 ) );
    s.addRow( d );
    d.put( "d" , new IntegerObj( 3 ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.INTEGER );
    }
  }

  @Test
  public void T_11() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new ShortObj( (short)1 ) );
    s.addRow( d );
    d.put( "d" , new ByteObj( (byte)1 ) );
    s.addRow( d );
    d.put( "d" , new IntegerObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new LongObj( 1 ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.LONG );
    }
  }

  @Test
  public void T_13() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new ShortObj( (short)1 ) );
    s.addRow( d );
    d.put( "d" , new ByteObj( (byte)1 ) );
    s.addRow( d );
    d.put( "d" , new IntegerObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new LongObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new FloatObj( 1 ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.UNION );
    }
  }

  @Test
  public void T_14() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new ShortObj( (short)1 ) );
    s.addRow( d );
    d.put( "d" , new ByteObj( (byte)1 ) );
    s.addRow( d );
    d.put( "d" , new IntegerObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new LongObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new StringObj( "1" ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.UNION );
    }
  }

  @Test
  public void T_15() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new FloatObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new DoubleObj( 1 ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.DOUBLE );
    }
  }

  @Test
  public void T_16() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new FloatObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new DoubleObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new IntegerObj( 1 ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.UNION );
    }
  }

  @Test
  public void T_17() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new FloatObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new DoubleObj( 1 ) );
    s.addRow( d );
    d.put( "d" , new StringObj( "1" ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.UNION );
    }
  }

  @Test
  public void T_18() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiWriter writer = new YosegiWriter( out , config );

    Spread s = new Spread();
    Map<String,Object> d = new HashMap<String,Object>();
    d.put( "d" , new BytesObj( new byte[0] ) );
    s.addRow( d );
    d.put( "d" , new StringObj( "1" ) );
    s.addRow( d );
    writer.append( s );
    writer.close();

    assertEquals( s.getColumn("d").getColumnType() , ColumnType.UNION );

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "d" );
      assertEquals( column.getColumnType() , ColumnType.UNION );
    }
  }

}
