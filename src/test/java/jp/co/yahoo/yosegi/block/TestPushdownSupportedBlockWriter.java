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

  private List<ColumnBinary> createTestData() throws IOException {
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
  public void T_appendAndSize_equalsAppendBinarySize() throws IOException {
    PushdownSupportedBlockWriter writer = new PushdownSupportedBlockWriter();
    writer.setup( 1024 * 1024 * 8 , new Configuration() );
    List<ColumnBinary> list = createTestData();
    int sizeAfterAppend = writer.sizeAfterAppend( list );
    writer.append( 10 , list );
    assertEquals( writer.size() , sizeAfterAppend );
    
  }

}
