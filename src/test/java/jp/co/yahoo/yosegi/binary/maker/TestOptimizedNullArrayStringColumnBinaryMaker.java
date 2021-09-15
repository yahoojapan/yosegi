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
package jp.co.yahoo.yosegi.binary.maker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;

import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.StringColumnAnalizeResult;

import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.Utf8BytesLinkObj;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

public class TestOptimizedNullArrayStringColumnBinaryMaker {

  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    int loadCount =
        (columnBinary.loadIndex == null) ? columnBinary.rowCount : columnBinary.loadIndex.length;
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  public IColumn makeColumn( final String[] strs ) throws IOException {
    PrimitiveColumn column = new PrimitiveColumn( ColumnType.STRING , "test" );
    for ( int i = 0,n = 0 ; i < strs.length ; i++, n+=300 ) {
      column.add( ColumnType.STRING , new StringObj( strs[i] ) , n );
    }
    return column;
  }

  public void check( final IColumn column , final String[] strs ) throws IOException {
    for ( int i = 0,n = 0 ; i < strs.length ; i++, n+=300 ) {
      PrimitiveObject obj = (PrimitiveObject)( column.get( n ).getRow() );
      assertEquals( strs[i] , obj.getString() );
    }
  }

  @Test
  public void T_veryLongValue() throws IOException {
    String[] strs = new String[10];
    strs[0] = "1235.599";
    strs[1] = "1435.599";
    strs[2] = "1435.599";
    strs[3] = "1435.699";
    StringBuffer buf = new StringBuffer();
    for ( int i = 0 ; i < 120000 ; i++ ) {
      buf.append( "1435.699&" );
    }
    strs[4] = buf.toString();
    strs[5] = "1860.640";
    strs[6] = "860.1640";
    StringBuffer buf2 = new StringBuffer();
    for ( int i = 0 ; i < 50000 ; i++ ) {
      buf2.append( "1435.699&" );
    }
    strs[7] = buf2.toString();
    strs[8] = "2860.640";
    strs[9] = "860.1640";

    IColumn column = makeColumn( strs );
    OptimizedNullArrayStringColumnBinaryMaker maker = new OptimizedNullArrayStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    IColumn newColumn = toColumn(columnBinary);
    check( newColumn , strs );
  }

}
