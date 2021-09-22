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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IArrayLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadArrayLink;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class MaxLengthBasedArrayColumnBinaryMaker implements IColumnBinaryMaker {

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    int maxSize = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell instanceof ArrayCell ) {
        ArrayCell arrayCell = (ArrayCell) cell;
        int arrayLength = arrayCell.getEnd() - arrayCell.getStart();
        if ( maxSize < arrayLength  ) {
          maxSize = arrayLength;
        }
      }
    }
    NumberToBinaryUtils.IIntConverter encoder = NumberToBinaryUtils.getIntConverter( 0 , maxSize );
    byte[] binaryRaw = new byte[Integer.BYTES + encoder.calcBinarySize( column.size() )];
    ByteBuffer.wrap( binaryRaw ).putInt( maxSize );
    IWriteSupporter writer = encoder.toWriteSuppoter(
        column.size() , binaryRaw , Integer.BYTES , encoder.calcBinarySize( column.size() ) );

    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell instanceof ArrayCell ) {
        ArrayCell arrayCell = (ArrayCell) cell;
        writer.putInt( arrayCell.getEnd() - arrayCell.getStart() );
      } else {
        writer.putInt( 0 );
      }
    }

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressData = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    IColumn childColumn = column.getColumn( 0 );
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();

    ColumnBinaryMakerCustomConfigNode childColumnConfigNode = null;
    IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
    if ( currentConfigNode != null ) {
      childColumnConfigNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
      if ( childColumnConfigNode != null ) {
        maker = childColumnConfigNode
            .getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
      }
    }
    columnBinaryList.add( maker.toBinary(
        commonConfig ,
        childColumnConfigNode ,
        compressResultNode.getChild( childColumn.getColumnName() ) ,
        childColumn ) );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.ARRAY ,
        column.size() ,
        binaryRaw.length ,
        0 ,
        -1 ,
        compressData ,
        0 ,
        compressData.length ,
        columnBinaryList );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return Integer.BYTES * analizeResult.getColumnSize();
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary , final int loadSize ) {
    return LoadType.ARRAY;
  }

  private void loadFromColumnBinary(
      final ColumnBinary columnBinary , final IArrayLoader loader ) throws IOException {
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] decompressBuffer = compressor.decompress(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int maxSize = ByteBuffer.wrap( decompressBuffer ).getInt();
    NumberToBinaryUtils.IIntConverter encoder = NumberToBinaryUtils.getIntConverter( 0 , maxSize );
    IReadSupporter reader = encoder.toReadSupporter(
        decompressBuffer , Integer.BYTES , decompressBuffer.length - Integer.BYTES );

    int currentIndex = 0;
    for ( int i = 0 ; i < loader.getLoadSize() ; i++ ) {
      if ( columnBinary.rowCount <= i ) {
        loader.setNull( i );
      }
      int arrayLength = reader.getInt();
      if ( arrayLength == 0 ) {
        loader.setNull(i);
      } else {
        loader.setArrayIndex( i , currentIndex , arrayLength );
        currentIndex += arrayLength;
      }
    }

    ColumnBinary child = columnBinary.columnBinaryList.get(0);
    loader.loadChild( child , currentIndex );
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary , final IArrayLoader loader ) throws IOException {
    if ( columnBinary.loadIndex.length == 0
        || columnBinary.rowCount <= columnBinary.loadIndex[0] ) {
      for ( int i = 0 ; i < loader.getLoadSize() ; i++ ) {
        loader.setNull(i);
      }
      return;
    }

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] decompressBuffer = compressor.decompress(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int maxSize = ByteBuffer.wrap( decompressBuffer ).getInt();
    NumberToBinaryUtils.IIntConverter encoder = NumberToBinaryUtils.getIntConverter( 0 , maxSize );
    IReadSupporter reader = encoder.toReadSupporter(
        decompressBuffer , Integer.BYTES , decompressBuffer.length - Integer.BYTES );

    int currentIndex = 0;
    boolean[] isNullArray = new boolean[columnBinary.rowCount];
    int[] startArray = new int[columnBinary.rowCount];
    int[] lengthArray = new int[columnBinary.rowCount];
    for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
      int arrayLength = reader.getInt();
      if ( arrayLength == 0 ) {
        isNullArray[i] = true;
      } else {
        startArray[i] = currentIndex;
        lengthArray[i] = arrayLength;
        currentIndex += arrayLength;
      }
    }

    for ( int loadIndex : columnBinary.loadIndex ) {
      if ( columnBinary.rowCount <= loadIndex || isNullArray[loadIndex] ) {
        loader.setNull(loadIndex);
      } else {
        loader.setArrayIndex( loadIndex , startArray[loadIndex] , lengthArray[loadIndex] );
      }
    }

    ColumnBinary child = columnBinary.columnBinaryList.get(0);
    loader.loadChild( child , child.loadIndex.length );
  }

  @Override
  public void load(
      final ColumnBinary columnBinary , final ILoader loader ) throws IOException {
    if ( loader.getLoaderType() != LoadType.ARRAY ) {
      throw new IOException( "Loader type is not ARRAY." );
    }

    if ( columnBinary.loadIndex == null ) {
      loadFromColumnBinary( columnBinary , (IArrayLoader)loader );
    } else {
      loadFromExpandColumnBinary( columnBinary , (IArrayLoader)loader );
    }
    loader.finish();
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    if ( columnBinary.columnBinaryList.isEmpty() ) {
      parentNode.getChildNode( columnBinary.columnName ).disable();
      return;
    }
    ColumnBinary childColumnBinary = columnBinary.columnBinaryList.get(0);
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
    if ( parentNode.containsKey( columnBinary.columnName ) ) {
      parentNode.putChildNode( 
          childColumnBinary.columnName , parentNode.getChildNode( columnBinary.columnName ) );

    }
    maker.setBlockIndexNode( parentNode , childColumnBinary , spreadIndex );
    parentNode.putChildNode( 
        columnBinary.columnName , parentNode.getChildNode( childColumnBinary.columnName ) );
    parentNode.deleteChildNode( childColumnBinary.columnName );
  }

}
