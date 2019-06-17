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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker;
import jp.co.yahoo.yosegi.block.PushdownSupportedBlockWriter;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.writer.YosegiWriter;

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
import java.util.List;

public class TestYosegiReader {

  private int blockSize = 1024 * 1024 * 4;

  private ColumnBinary createStringPushdownTestColumn() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 3 );

    IColumnBinaryMaker maker = new UnsafeOptimizeDumpStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    return maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
  }

  private ColumnBinary createStringPushdownTestColumn2() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column2" );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "D" ) , 3 );

    IColumnBinaryMaker maker = new UnsafeOptimizeDumpStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    return maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
  }

  private byte[] createTestBinary() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration writerConfig = new Configuration();
    writerConfig.set( "block.size" , Integer.toString( blockSize ) );
    YosegiWriter writer = new YosegiWriter( out , writerConfig );
    List<ColumnBinary> list = Arrays.asList( createStringPushdownTestColumn() );
    // Block-1 Spread-1
    writer.appendRow( list , 4 );
    // Block-1 Spread-2
    writer.appendRow( list , 4 );
    writer.writeFixedBlock();
    // Block-2 Spread-1
    writer.appendRow( list , 4 );
    // Block-2 Spread-2
    writer.appendRow( list , 4 );
    writer.writeFixedBlock();
    // Block-3 Spread-1
    writer.appendRow( Arrays.asList( createStringPushdownTestColumn2() ) , 4 );

    writer.close();
    out.close();
    return out.toByteArray();
  }

  @Test
  public void T_read_1() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    reader.setNewStream( in , blocks.length , new Configuration() );
    assertEquals( reader.hasNext() , true );

    // Block-1 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    // Block-1 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );

    // Block-2 Spread-1
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    // Block-2 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );

    // Block-3 Spread-1
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );

    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_EmptyPushdown_1() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    Configuration readerConfig = new Configuration();
    // Skip Block-1, Block-2
    reader.setBlockSkipIndex( index );
    reader.setNewStream( in , blocks.length , readerConfig );
    assertEquals( reader.hasNext() , true );

    // Block-3 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_EmptyPushdown_2() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"column\"]]" );
    // Skip Block-1, Block-2
    reader.setBlockSkipIndex( index );
    reader.setNewStream( in , blocks.length , readerConfig );
    assertEquals( reader.hasNext() , true );
    // Block-3 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( reader.getBlockReadCount() , 1 );
    assertEquals( raw.size() , 0 );
    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_EmptyPushdown_3() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column2" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"column2\"]]" );
    reader.setBlockSkipIndex( index );
    // Skip Block-3
    reader.setNewStream( in , blocks.length , readerConfig );
    assertEquals( reader.hasNext() , true );

    // Block-1 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( reader.hasNext() , true );

    // Block-1 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( reader.hasNext() , true );

    // Block-2 Spread-1
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( reader.hasNext() , true );

    // Block-2 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_EmptyPushdownAndBlockRead_1() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    Configuration readerConfig = new Configuration();
    // Skip Block-1, Block-2
    reader.setBlockSkipIndex( index );
    // Read Block-1, Block-2
    reader.setNewStream( in , blocks.length , readerConfig , 0 , blockSize * 2 );
    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_EmptyPushdownAndBlockRead_2() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    Configuration readerConfig = new Configuration();
    // Skip Block-1, Block-2
    reader.setBlockSkipIndex( index );
    // Read Block-2
    reader.setNewStream( in , blocks.length , readerConfig , blockSize , blockSize );
    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_EmptyPushdownAndBlockRead_3() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    Configuration readerConfig = new Configuration();
    // Skip Block-1, Block-2
    reader.setBlockSkipIndex( index );
    // Read Block-3
    reader.setNewStream( in , blocks.length , readerConfig , blockSize * 2 , blockSize );

    assertEquals( reader.hasNext() , true );
    // Block-3 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );

    assertEquals( reader.hasNext() , false );
  }

  @Test
  public void T_ReadWithoutBlock_1() throws IOException {
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "D" ) )
    );
    Configuration readerConfig = new Configuration();
    reader.setBlockSkipIndex( index );
    // There is no block to read.
    reader.setNewStream( in , blocks.length , readerConfig , 1024 * 256 , 1024 * 256 );
    assertEquals( reader.hasNext() , false );
  }

}
