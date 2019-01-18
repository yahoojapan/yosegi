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
import jp.co.yahoo.yosegi.binary.maker.index.RangeFloatIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.FloatRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.DataType;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

public class UnsafeRangeDumpFloatColumnBinaryMaker implements IColumnBinaryMaker {

  private static final int HEADER_SIZE = ( Float.BYTES * 2 ) + Byte.BYTES;

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    byte[] parentsBinaryRaw = 
        new byte[ Byte.BYTES + column.size() + ( column.size() * Float.BYTES ) ];
    ByteOrder order = ByteOrder.nativeOrder();

    IWriteSupporter nullSupporter = ByteBufferSupporterFactory.createWriteSupporter(
        parentsBinaryRaw , 0 , column.size() , order );
    IWriteSupporter doubleSupporter = ByteBufferSupporterFactory.createWriteSupporter(
        parentsBinaryRaw ,  Byte.BYTES + column.size() , column.size() * Float.BYTES , order );

    int rowCount = 0;
    boolean hasNull = false;
    Float min = Float.MAX_VALUE;
    Float max = Float.MIN_VALUE;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullSupporter.putByte( (byte)1 );
        hasNull = true;
      } else {
        rowCount++;
        PrimitiveCell byteCell = (PrimitiveCell) cell;
        nullSupporter.putByte( (byte)0 );
        Float target = Float.valueOf( byteCell.getRow().getFloat() );
        doubleSupporter.putFloat( target );
        if ( 0 < min.compareTo( target ) ) {
          min = Float.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Float.valueOf( target );
        }
      }
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new FloatObj( min ) , column.getColumnName() , column.size() );
    }

    int rawLength;
    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;
    parentsBinaryRaw[column.size()] = byteOrderByte;

    byte[] compressBinaryRaw;
    if ( hasNull ) {
      rawLength =  parentsBinaryRaw.length - ( Float.BYTES * ( column.size() - rowCount ) );
      compressBinaryRaw = currentConfig.compressorClass.compress(
          parentsBinaryRaw , 0 , rawLength , DataType.NUMBER );
    } else {
      rawLength = Byte.BYTES + column.size() * Float.BYTES;
      compressBinaryRaw = currentConfig.compressorClass.compress(
          parentsBinaryRaw ,
          column.size() ,
          parentsBinaryRaw.length - column.size() ,
          DataType.NUMBER );
    }
    byte[] binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    wrapBuffer.putFloat( min );
    wrapBuffer.putFloat( max );
    wrapBuffer.put( hasNull ? (byte)1 : (byte)0 );
    wrapBuffer.put( compressBinaryRaw );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.DOUBLE ,
        column.size() ,
        rawLength ,
        rowCount * Float.BYTES ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    if ( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ) {
      return Float.BYTES;
    } else if ( analizeResult.getNullCount() == 0 ) {
      return analizeResult.getColumnSize() * Float.BYTES;
    } else {
      return analizeResult.getColumnSize() 
          + ( analizeResult.getColumnSize() - analizeResult.getNullCount() ) * Float.BYTES;
    }
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    Float min = Float.valueOf( wrapBuffer.getFloat() );
    Float max = Float.valueOf( wrapBuffer.getFloat() );
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new RangeFloatColumnManager( columnBinary ),
        new RangeFloatIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException {
    byte type = columnBinary.binary[ columnBinary.binaryStart + Float.BYTES * 2 ];
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress(
        columnBinary.binary ,
        columnBinary.binaryStart + HEADER_SIZE ,
        columnBinary.binaryLength - HEADER_SIZE );
    if ( type == (byte)1 ) {
      ByteOrder order = binary[columnBinary.rowCount] == (byte)0
          ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

      IReadSupporter nullReader = ByteBufferSupporterFactory.createReadSupporter(
          binary ,
          0 ,
          columnBinary.rowCount ,
          order );

      IReadSupporter doubleReader = ByteBufferSupporterFactory.createReadSupporter(
          binary ,
          columnBinary.rowCount + Byte.BYTES ,
          Float.BYTES * columnBinary.rowCount ,

          order );
      for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
        if ( nullReader.getByte() == (byte)0 ) {
          allocator.setFloat( i , doubleReader.getFloat() );
        } else {
          allocator.setNull( i );
        }
      }
    } else {
      ByteOrder order = binary[0] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      IReadSupporter doubleReader = ByteBufferSupporterFactory.createReadSupporter(
            binary ,
            Byte.BYTES ,
            Float.BYTES * columnBinary.rowCount ,
            order );
      for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
        allocator.setFloat( i , doubleReader.getFloat() );
      }
    }

    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    Float min = Float.valueOf( wrapBuffer.getFloat() );
    Float max = Float.valueOf( wrapBuffer.getFloat() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new FloatRangeBlockIndex( min , max ) );
  }

  public class RangeFloatDicManager implements IDicManager {

    private final PrimitiveObject[] doubleArray;

    public RangeFloatDicManager( final PrimitiveObject[] doubleArray ) {
      this.doubleArray = doubleArray;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException {
      return doubleArray[index];
    }

    @Override
    public int getDicSize() throws IOException {
      return doubleArray.length;
    }

  }

  public class RangeFloatColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private PrimitiveColumn column;
    private boolean isCreate;

    public RangeFloatColumnManager( final ColumnBinary columnBinary ) throws IOException {
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      PrimitiveObject[] array = new PrimitiveObject[columnBinary.rowCount];
      byte type = columnBinary.binary[ columnBinary.binaryStart + Float.BYTES * 2 ];
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress(
          columnBinary.binary ,
          columnBinary.binaryStart + HEADER_SIZE ,
          columnBinary.binaryLength - HEADER_SIZE );
      if ( type == (byte)1 ) {
        ByteOrder order = 
            binary[columnBinary.rowCount] == (byte)0
            ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        IReadSupporter nullReader = ByteBufferSupporterFactory.createReadSupporter(
            binary ,
            0 ,
            columnBinary.rowCount ,
            order );

        IReadSupporter doubleReader = ByteBufferSupporterFactory.createReadSupporter(
            binary ,
            columnBinary.rowCount + Byte.BYTES , 
            Float.BYTES * columnBinary.rowCount , 
            order );

        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          if ( nullReader.getByte() == (byte)0 ) {
            array[i] = new FloatObj( doubleReader.getFloat() );
          }
        }
      } else if ( type == (byte)0) {
        ByteOrder order = binary[0] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        IReadSupporter doubleReader = ByteBufferSupporterFactory.createReadSupporter(
            binary , 
            Byte.BYTES , 
            Float.BYTES * columnBinary.rowCount , 
            order );
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          array[i] = new FloatObj( doubleReader.getFloat() );
        }
      }

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      IDicManager dicManager = new RangeFloatDicManager( array );
      column.setCellManager( 
          new BufferDirectCellManager( 
            columnBinary.columnType , dicManager , columnBinary.rowCount 
          ) );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      try {
        create(); 
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
      }
      return column;
    }

    @Override
    public List<String> getColumnKeys() {
      return new ArrayList<String>();
    }

    @Override
    public int getColumnSize() {
      return 0;
    }

  }

}
