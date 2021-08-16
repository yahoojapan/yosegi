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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.maker.DumpBooleanColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.DefaultCompressor;
import jp.co.yahoo.yosegi.compressor.GzipCompressor;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.expression.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPushdownSupportedBlockWriter {

  private List<ColumnBinary> createCompressMetaCaseData() throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "c" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 0 );

    DumpBooleanColumnBinaryMaker maker = new DumpBooleanColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    return Arrays.asList(
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column ) );
  }

  private List<ColumnBinary> createSingleCaseData() throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column1" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );

    UnsafeOptimizeDumpStringColumnBinaryMaker maker = new UnsafeOptimizeDumpStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    return Arrays.asList(
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column ) );
  }

  private List<ColumnBinary> createSimpleCaseData() throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column1" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );

    IColumn column2 = new PrimitiveColumn( ColumnType.STRING , "column2" );
    column2.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column2.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column2.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column2.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column2.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column2.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column2.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column2.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column2.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column2.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );

    UnsafeOptimizeDumpStringColumnBinaryMaker maker = new UnsafeOptimizeDumpStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    return Arrays.asList(
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column ) ,
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column2 ) );

  }

  @Test
  public void T_appendAndSize_equalsAppendBinarySize_withOnceList() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> list = createSimpleCaseData();
    int sizeAfterAppend = writer.sizeAfterAppend( list );
    writer.append( 10 , list );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_appendAndSize_equalsAppendBinarySize_withSomeList() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> list = createSimpleCaseData();
    writer.append( 10 , list );
    writer.append( 10 , list );
    writer.append( 10 , list );
    writer.append( 10 , list );
    int sizeAfterAppend = writer.sizeAfterAppend( list );
    writer.append( 10 , list );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_appendAndSize_equalsAppendBinarySize_withOneColumnAfterTwoColumns() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> list = createSingleCaseData();
    writer.append( 10 , list );
    List<ColumnBinary> list2 = createSimpleCaseData();
    int sizeAfterAppend = writer.sizeAfterAppend( list2 );
    writer.append( 10 , list2 );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_appendAndSize_equalsAppendBinarySize_withTwoColumnsAfterOneColumn() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> list = createSimpleCaseData();
    writer.append( 10 , list );
    List<ColumnBinary> list2 = createSingleCaseData();
    int sizeAfterAppend = writer.sizeAfterAppend( list2 );
    writer.append( 10 , list2 );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedSimpleCaseChild() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    int sizeAfterAppend = writer.sizeAfterAppend( list );
    writer.append( 10 , list );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withSomeNestedSimpleCaseChild() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    writer.append( 10 , list );
    writer.append( 10 , list );
    writer.append( 10 , list );
    writer.append( 10 , list );
    int sizeAfterAppend = writer.sizeAfterAppend( list );
    writer.append( 10 , list );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedFirstOneColumnAfterTwoColumn() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> childList = createSingleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    writer.append( 10 , list );

    List<ColumnBinary> childList2 = createSimpleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int sizeAfterAppend = writer.sizeAfterAppend( list2 );
    writer.append( 10 , list2 );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedFirstTwoColumnsAfterOneColumn() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    writer.append( 10 , list );

    List<ColumnBinary> childList2 = createSingleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int sizeAfterAppend = writer.sizeAfterAppend( list2 );
    writer.append( 10 , list2 );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedFirstIsNoting() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> list = createSimpleCaseData();
    writer.append( 10 , list );

    List<ColumnBinary> childList2 = createSimpleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int sizeAfterAppend = writer.sizeAfterAppend( list2 );
    writer.append( 10 , list2 );
    assertEquals( writer.size() , sizeAfterAppend );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( outputDataSize , block.length );
  }

  @Test
  public void T_blockSizeEqualsBlockBinarySize_equalsBlockSizeAndRead_withVariableBlock() throws IOException {
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    tmpWriter.setup( 1024 * 1024 * 8 , new Configuration() );

    List<ColumnBinary> list = createSimpleCaseData();
    tmpWriter.append( 10 , list );
    tmpWriter.append( 10 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize , new Configuration() );
    writer.append( 10 , list );
    assertTrue( writer.canAppend( list ) );
    writer.append( 10 , list );
    assertEquals( writer.size() , blockSize );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( blockSize , block.length );

    PushdownSupportedBlockReader reader = new PushdownSupportedBlockReader();
    ByteArrayInputStream in = new ByteArrayInputStream( block );
    reader.setup( new Configuration() );
    reader.setStream( in , blockSize );
    assertEquals( 2 , reader.getBlockCount() );
    while ( reader.hasNext() ) {
      Spread spread = reader.next();
      IColumn c1 = spread.getColumn( "column1" );
      PrimitiveObject[] c1Array = c1.getPrimitiveObjectArray( 0 , 10 );
      assertEquals( c1Array[0].getString() , "a" );
      assertEquals( c1Array[1].getString() , "ab" );
      assertEquals( c1Array[2].getString() , "abc" );
      assertEquals( c1Array[3].getString() , "abcd" );
      assertEquals( c1Array[4].getString() , "b" );
      assertEquals( c1Array[5].getString() , "bc" );
      assertEquals( c1Array[6].getString() , "bcd" );
      assertEquals( c1Array[7].getString() , "bcde" );
      assertEquals( c1Array[8].getString() , "c" );
      assertEquals( c1Array[9].getString() , "cd" );
      IColumn c2 = spread.getColumn( "column2" );
      PrimitiveObject[] c2Array = c1.getPrimitiveObjectArray( 0 , 10 );
      assertEquals( c2Array[0].getString() , "a" );
      assertEquals( c2Array[1].getString() , "ab" );
      assertEquals( c2Array[2].getString() , "abc" );
      assertEquals( c2Array[3].getString() , "abcd" );
      assertEquals( c2Array[4].getString() , "b" );
      assertEquals( c2Array[5].getString() , "bc" );
      assertEquals( c2Array[6].getString() , "bcd" );
      assertEquals( c2Array[7].getString() , "bcde" );
      assertEquals( c2Array[8].getString() , "c" );
      assertEquals( c2Array[9].getString() , "cd" );
    }
    reader.close();
  }

  @Test
  public void T_blockSizeEqualsBlockBinarySize_equalsBlockSizeAndRead_withFixedBlock() throws IOException {
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    tmpWriter.setup( 1024 * 1024 * 8 , new Configuration() );

    List<ColumnBinary> list = createSimpleCaseData();
    tmpWriter.append( 10 , list );
    tmpWriter.append( 10 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize , new Configuration() );
    writer.append( 10 , list );
    assertTrue( writer.canAppend( list ) );
    writer.append( 10 , list );
    assertEquals( writer.size() , blockSize );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeFixedBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( blockSize , block.length );

    PushdownSupportedBlockReader reader = new PushdownSupportedBlockReader();
    ByteArrayInputStream in = new ByteArrayInputStream( block );
    reader.setup( new Configuration() );
    reader.setStream( in , blockSize );
    assertEquals( 2 , reader.getBlockCount() );
    while ( reader.hasNext() ) {
      Spread spread = reader.next();
      IColumn c1 = spread.getColumn( "column1" );
      PrimitiveObject[] c1Array = c1.getPrimitiveObjectArray( 0 , 10 );
      assertEquals( c1Array[0].getString() , "a" );
      assertEquals( c1Array[1].getString() , "ab" );
      assertEquals( c1Array[2].getString() , "abc" );
      assertEquals( c1Array[3].getString() , "abcd" );
      assertEquals( c1Array[4].getString() , "b" );
      assertEquals( c1Array[5].getString() , "bc" );
      assertEquals( c1Array[6].getString() , "bcd" );
      assertEquals( c1Array[7].getString() , "bcde" );
      assertEquals( c1Array[8].getString() , "c" );
      assertEquals( c1Array[9].getString() , "cd" );
      IColumn c2 = spread.getColumn( "column2" );
      PrimitiveObject[] c2Array = c1.getPrimitiveObjectArray( 0 , 10 );
      assertEquals( c2Array[0].getString() , "a" );
      assertEquals( c2Array[1].getString() , "ab" );
      assertEquals( c2Array[2].getString() , "abc" );
      assertEquals( c2Array[3].getString() , "abcd" );
      assertEquals( c2Array[4].getString() , "b" );
      assertEquals( c2Array[5].getString() , "bc" );
      assertEquals( c2Array[6].getString() , "bcd" );
      assertEquals( c2Array[7].getString() , "bcde" );
      assertEquals( c2Array[8].getString() , "c" );
      assertEquals( c2Array[9].getString() , "cd" );
    }
    reader.close();
  }

  @Test
  public void T_blockSizeEqualsHeaderAppendBlockBinary_equals_withVariableBlock() throws IOException {
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    tmpWriter.setup( 1024 * 1024 * 8 , new Configuration() );

    List<ColumnBinary> list = createSimpleCaseData();
    tmpWriter.append( 10 , list );
    tmpWriter.append( 10 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    byte[] header = "header".getBytes();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize + header.length , new Configuration() );
    writer.appendHeader( header );
    writer.append( 10 , list );
    assertTrue( writer.canAppend( list ) );
    writer.append( 10 , list );
    assertEquals( writer.size() , blockSize + header.length );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeVariableBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( blockSize + header.length , block.length );
  }

  @Test
  public void T_blockSizeEqualsHeaderAppendBlockBinary_equals_withFixedBlock() throws IOException {
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    tmpWriter.setup( 1024 * 1024 * 8 , new Configuration() );

    List<ColumnBinary> list = createSimpleCaseData();
    tmpWriter.append( 10 , list );
    tmpWriter.append( 10 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    byte[] header = "header".getBytes();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize + header.length , new Configuration() );
    writer.appendHeader( header );
    writer.append( 10 , list );
    assertTrue( writer.canAppend( list ) );
    writer.append( 10 , list );
    assertEquals( writer.size() , blockSize + header.length );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeFixedBlock( out );
    byte[] block = out.toByteArray();
    assertEquals( blockSize + header.length , block.length );
  }

  @Test
  public void T_bufferOverflow_BlockSizeExceededAfterAddition() throws IOException {
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    tmpWriter.setup( 1024 * 1024 * 8 , new Configuration() );

    List<ColumnBinary> list = createSimpleCaseData();
    tmpWriter.append( 10 , list );
    tmpWriter.append( 10 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize , new Configuration() );
    writer.append( 10 , list );
    assertTrue( writer.canAppend( list ) );
    writer.append( 10 , list );
    assertEquals( writer.size() , blockSize );
    assertFalse( writer.canAppend( list ) );
    assertThrows( IOException.class ,
      () -> {
        writer.append( 10 , list );
      }
    );
  }

  @Test
  public void T_canAppend_throwsException_whenColumnBinaryTreeIsEmpty() throws IOException {
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    tmpWriter.setup( 1024 * 1024 * 8 , new Configuration() );

    List<ColumnBinary> list = createSimpleCaseData();
    tmpWriter.append( 10 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize - 1 , new Configuration() );
    assertThrows( IOException.class ,
      () -> {
        writer.canAppend( list );
      }
    );
  }

  @Test
  public void T_writeFixedBlock_throwsException_whenBlockSizeIsSmallAndMetaBinaryAfterCompressIsLargerThanOriginal() throws IOException {
    // If the meta is small, the size after compression may be large.
    // Because the block size is usually in MB units, the possibility of the meta becoming small is extremely low.
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    Configuration config = new Configuration();
    config.set( "block.maker.compress.class" , BlockTestCompressor.class.getName() );
    tmpWriter.setup( 1024 * 1024 * 8 , config );

    List<ColumnBinary> list = createCompressMetaCaseData();
    tmpWriter.append( 1 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize , config );
    writer.canAppend( list );
    writer.append( 1 , list );
    assertEquals( writer.size() , blockSize );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertThrows( IOException.class ,
      () -> {
        writer.writeFixedBlock( out );
      }
    );
    // This is invalid block.
    byte[] block = out.toByteArray();
    assertEquals( blockSize + 1 , block.length );
  }

  @Test
  public void T_writeVariableBlock_throwsException_whenBlockSizeIsSmallAndMetaBinaryAfterCompressIsLargerThanOriginal() throws IOException {
    // If the meta is small, the size after compression may be large.
    // Because the block size is usually in MB units, the possibility of the meta becoming small is extremely low.
    PushdownSupportedBlockWriter tmpWriter = new PushdownSupportedBlockWriter();
    Configuration config = new Configuration();
    config.set( "block.maker.compress.class" , BlockTestCompressor.class.getName() );
    tmpWriter.setup( 1024 * 1024 * 8 , config );

    List<ColumnBinary> list = createCompressMetaCaseData();
    tmpWriter.append( 1 , list );
    int blockSize = tmpWriter.size();
    tmpWriter.close();

    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize , config );
    writer.canAppend( list );
    writer.append( 1 , list );
    assertEquals( writer.size() , blockSize );

    int outputDataSize = writer.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertThrows( IOException.class ,
      () -> {
        writer.writeVariableBlock( out );
      }
    );
    // This is invalid block.
    byte[] block = out.toByteArray();
    assertEquals( blockSize + 1 , block.length );
  }

}
