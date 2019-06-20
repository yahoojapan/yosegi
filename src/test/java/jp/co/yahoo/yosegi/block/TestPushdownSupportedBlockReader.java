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
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
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
import java.util.List;

public class TestPushdownSupportedBlockReader {

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

  @Test
  public void T_hasNext_1() throws IOException {
    PushdownSupportedBlockReader reader = new PushdownSupportedBlockReader();
    assertFalse( reader.hasNext() );
  }

  @Test
  public void T_hasNext_2() throws IOException {
    PushdownSupportedBlockReader reader = new PushdownSupportedBlockReader();
    reader.setup( new Configuration() );
    assertFalse( reader.hasNext() );
  }

  @Test
  public void T_EmptyPushdown_1() throws IOException {
    int blockSize = 1024 * 1024 * 4;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( blockSize , new Configuration() );
    List<ColumnBinary> list = Arrays.asList( createStringPushdownTestColumn() );
    writer.append( 4 , list );
    writer.append( 4 , list );
    writer.writeFixedBlock( out );
    writer.append( 4 , list );
    writer.append( 4 , list );
    writer.writeFixedBlock( out );
    writer.append( 4 , list );
    writer.append( 4 , list );
    writer.writeVariableBlock( out );

    writer.close();
    out.close();

    byte[] block = out.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream( block );
    PushdownSupportedBlockReader reader = new PushdownSupportedBlockReader();
    reader.setup( new Configuration() );
    AndExpressionNode index = new AndExpressionNode();
    index.addChildNode(
        new ExecuterNode( new StringExtractNode( "column" )
        , new PerfectMatchStringFilter( "p" ) )
    );
    reader.setBlockSkipIndex( index );
    reader.setStream( in , blockSize );
    assertEquals( reader.hasNext() , false );
    assertEquals( reader.getBlockCount() , 0 );
    reader.setStream( in , blockSize );
    assertEquals( reader.hasNext() , false );
    assertEquals( reader.getBlockCount() , 0 );
    reader.setStream( in , blockSize );
    assertEquals( reader.hasNext() , false );
    assertEquals( reader.getBlockCount() , 0 );
  }

}
