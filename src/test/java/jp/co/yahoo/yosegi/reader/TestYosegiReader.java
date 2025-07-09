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
import jp.co.yahoo.yosegi.binary.maker.*;
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

    IColumnBinaryMaker maker = new OptimizedNullArrayDumpStringColumnBinaryMaker();
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

    IColumnBinaryMaker maker = new OptimizedNullArrayDumpStringColumnBinaryMaker();
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
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
    byte[] blocks = createTestBinary();
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    YosegiReader reader = new YosegiReader();
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );

    reader.setNewStream( in , blocks.length , new Configuration() );
    // File Header size is 118B
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 407, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 407, reader.getReadBytes() );

    // Block-1 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 407, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 407, reader.getReadBytes() );
    // Block-1 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 2, reader.getBlockReadCount() );
    assertEquals( 407, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 2, reader.getBlockReadCount() );
    assertEquals( 696, reader.getReadBytes() );

    // Block-2 Spread-1
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 3, reader.getBlockReadCount() );
    assertEquals( 696, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 3, reader.getBlockReadCount() );
    assertEquals( 696, reader.getReadBytes() );
    // Block-2 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 4, reader.getBlockReadCount() );
    assertEquals( 696, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 5, reader.getBlockCount() );
    assertEquals( 4, reader.getBlockReadCount() );
    assertEquals( 905, reader.getReadBytes() );

    // Block-3 Spread-1
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 5, reader.getBlockCount() );
    assertEquals( 5, reader.getBlockReadCount() );
    assertEquals( 905, reader.getReadBytes() );

    assertEquals( reader.hasNext() , false );
    assertEquals( 5, reader.getBlockCount() );
    assertEquals( 5, reader.getBlockReadCount() );
    assertEquals( 905, reader.getReadBytes() );
  }

  @Test
  public void T_EmptyPushdown_1() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );

    reader.setNewStream( in , blocks.length , readerConfig );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118 + 71 + 71 + (72 + 132 + 5) = 469
    assertEquals( 469, reader.getReadBytes() );

    assertEquals( reader.hasNext() , true );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 469, reader.getReadBytes() );

    // Block-3 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 469, reader.getReadBytes() );

    assertEquals( reader.hasNext() , false );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 469, reader.getReadBytes() );
  }

  @Test
  public void T_EmptyPushdown_2() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );

    reader.setNewStream( in , blocks.length , readerConfig );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118 + 71 + 71 + (72 + 132) = 464
    assertEquals( 464, reader.getReadBytes() );

    assertEquals( reader.hasNext() , true );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 464, reader.getReadBytes() );
    // Block-3 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 464, reader.getReadBytes() );
    assertEquals( raw.size() , 0 );

    assertEquals( reader.hasNext() , false );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 464, reader.getReadBytes() );
  }

  @Test
  public void T_EmptyPushdown_3() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );

    // Skip Block-3
    reader.setNewStream( in , blocks.length , readerConfig );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118 + (71 + 208) = 397
    assertEquals( 397, reader.getReadBytes() );

    assertEquals( reader.hasNext() , true );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 397, reader.getReadBytes() );

    // Block-1 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 397, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 397, reader.getReadBytes() );

    // Block-1 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( 2, reader.getBlockCount() );
    assertEquals( 2, reader.getBlockReadCount() );
    assertEquals( 397, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 2, reader.getBlockReadCount() );
    // 118 + (71 + 208) + (71 + 208) = 676
    assertEquals( 676, reader.getReadBytes() );

    // Block-2 Spread-1
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 3, reader.getBlockReadCount() );
    assertEquals( 676, reader.getReadBytes() );
    assertEquals( reader.hasNext() , true );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 3, reader.getBlockReadCount() );
    assertEquals( 676, reader.getReadBytes() );

    // Block-2 Spread-2
    raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 0 );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 4, reader.getBlockReadCount() );
    assertEquals( 676, reader.getReadBytes() );

    assertEquals( reader.hasNext() , false );
    assertEquals( 4, reader.getBlockCount() );
    assertEquals( 4, reader.getBlockReadCount() );
    // 118 + (71 + 208) + (71 + 208) + 72 = 748
    assertEquals( 748, reader.getReadBytes() );
  }

  @Test
  public void T_EmptyPushdownAndBlockRead_1() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );
    // Read Block-1, Block-2
    reader.setNewStream( in , blocks.length , readerConfig , 0 , blockSize * 2 );
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118 + 71 + 71 = 260
    assertEquals( 260, reader.getReadBytes() );
    assertEquals( reader.hasNext() , false );
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 260, reader.getReadBytes() );
  }

  @Test
  public void T_EmptyPushdownAndBlockRead_2() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );
    // Read Block-2
    reader.setNewStream( in , blocks.length , readerConfig , blockSize , blockSize );
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118 + 71 = 189
    assertEquals( 189, reader.getReadBytes() );
    assertEquals( reader.hasNext() , false );
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 189, reader.getReadBytes() );
  }

  @Test
  public void T_EmptyPushdownAndBlockRead_3() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );
    // Read Block-3
    reader.setNewStream( in , blocks.length , readerConfig , blockSize * 2 , blockSize );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118 + (72 + 132 + 5) = 327
    assertEquals( 327, reader.getReadBytes() );

    assertEquals( reader.hasNext() , true );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 327, reader.getReadBytes() );
    // Block-3 Spread-1
    List<ColumnBinary> raw = reader.nextRaw();
    assertEquals( reader.getCurrentSpreadSize().intValue() , 4 );
    assertEquals( raw.size() , 1 );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 327, reader.getReadBytes() );

    assertEquals( reader.hasNext() , false );
    assertEquals( 1, reader.getBlockCount() );
    assertEquals( 1, reader.getBlockReadCount() );
    assertEquals( 327, reader.getReadBytes() );
  }

  @Test
  public void T_ReadWithoutBlock_1() throws IOException {
    // File Header: 118
    // Block-1: Meta: 71, Spread: 208, Offset: 10
    // Block-2: Meta: 71, Spread: 208, Offset: 10
    // Block-3: Meta: 72, Spread: 132, Offset: 5
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
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 0, reader.getReadBytes() );
    // There is no block to read.
    reader.setNewStream( in , blocks.length , readerConfig , 1024 * 256 , 1024 * 256 );
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    // 118
    assertEquals( 118, reader.getReadBytes() );
    assertEquals( reader.hasNext() , false );
    assertEquals( 0, reader.getBlockCount() );
    assertEquals( 0, reader.getBlockReadCount() );
    assertEquals( 118, reader.getReadBytes() );
  }

}
