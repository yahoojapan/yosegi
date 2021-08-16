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
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.DoubleColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnsafeOptimizeDoubleColumnBinaryMaker implements IColumnBinaryMaker {

  public static IDictionaryMaker chooseDictionaryMaker( final double min , final double max ) {
    return new DoubleDictionaryMaker();
  }

  /**
   * Select the smallest numeric type in the range of Index.
   */
  public static IDictionaryIndexMaker chooseDictionaryIndexMaker( final int dicIndexLength ) {
    if ( dicIndexLength <= NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) {
      return new ByteDictionaryIndexMaker();
    } else if ( dicIndexLength <= NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) {
      return new ShortDictionaryIndexMaker();
    } else {
      return new IntDictionaryIndexMaker();
    }
  }

  public interface IDictionaryMaker {

    int getLogicalSize( final int indexLength );

    int calcBinarySize( final int dicSize );

    void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

    PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

  }

  public static class DoubleDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Double.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Double.BYTES * dicSize;
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putDouble( obj.getDouble() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Double.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      wrapBuffer.getDouble();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new DoubleObj( wrapBuffer.getDouble() );
      }

      return result;
    }

  }

  public interface IDictionaryIndexMaker {

    int calcBinarySize( final int indexLength );

    void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

    IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

  }

  public static class ByteDictionaryIndexMaker implements IDictionaryIndexMaker {

    @Override
    public int calcBinarySize( final int indexLength ) {
      return Byte.BYTES * indexLength;
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int index : dicIndexArray ) {
        if ( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH < index ) {
          throw new IOException( "Invalid index number." );
        }
        wrapBuffer.putByte( (byte)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Byte.BYTES;
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( NumberToBinaryUtils.getUnsignedByteToInt( wrapBuffer.getByte() ) );
      }
      result.position( 0 );
      return result;
    }

  }

  public static class ShortDictionaryIndexMaker implements IDictionaryIndexMaker {

    @Override
    public int calcBinarySize( final int indexLength ) {
      return Short.BYTES * indexLength;
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int index : dicIndexArray ) {
        if ( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH < index ) {
          throw new IOException( "Invalid index number." );
        }
        wrapBuffer.putShort( (short)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Short.BYTES;
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( NumberToBinaryUtils.getUnsignedShortToInt( wrapBuffer.getShort() ) );
      }
      result.position( 0 );
      return result;
    }

  }

  public static class IntDictionaryIndexMaker implements IDictionaryIndexMaker {

    @Override
    public int calcBinarySize( final int indexLength ) {
      return Integer.BYTES * indexLength;
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putInt( index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        ByteOrder order ) throws IOException {
      int size = length / Integer.BYTES;
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( wrapBuffer.getInt() );
      }
      result.position( 0 );
      return result;
    }

  }

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
    Map<Double,Integer> dicMap = new HashMap<Double,Integer>();
    List<PrimitiveObject> dicList = new ArrayList<PrimitiveObject>();
    int[] indexArray = new int[column.size()];

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( new DoubleObj( (double)0 ) );

    Double min = Double.MAX_VALUE;
    Double max = Double.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      Double target = null;
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
      } else {
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        target = Double.valueOf( primitiveObj.getDouble() );
      }
      if ( ! dicMap.containsKey( target ) ) {
        if ( 0 < min.compareTo( target ) ) {
          min = Double.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Double.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicList.add( primitiveObj );
      }
      indexArray[i] = dicMap.get( target );
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new DoubleObj( min ) , column.getColumnName() , column.size() );
    }

    ByteOrder order = ByteOrder.nativeOrder();

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( indexArray.length );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.doubleValue() , max.doubleValue() );

    int indexLength = indexMaker.calcBinarySize( indexArray.length );
    int dicLength = dicMaker.calcBinarySize( dicList.size() );

    byte[] binaryRaw = new byte[ indexLength + dicLength ];
    indexMaker.create( indexArray , binaryRaw , 0 , indexLength , order );
    dicMaker.create( dicList , binaryRaw , indexLength , dicLength , order );

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary = new byte[ Double.BYTES * 2 + Byte.BYTES + compressBinary.length ];

    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putDouble( min );
    wrapBuffer.putDouble( max );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binary.length ,
        dicMaker.getLogicalSize( rowCount ) ,
        dicList.size() ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    double min = ( (DoubleColumnAnalizeResult) analizeResult ).getMin();
    double max = ( (DoubleColumnAnalizeResult) analizeResult ).getMax();
    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( analizeResult.getColumnSize() );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min , max );

    int indexLength = indexMaker.calcBinarySize( analizeResult.getColumnSize() );
    int dicLength = dicMaker.calcBinarySize( analizeResult.getUniqCount() );

    return indexLength + dicLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.doubleValue() , max.doubleValue() );
    return new LazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ColumnManager(
        columnBinary ,
        indexMaker ,
        dicMaker ,
        order
      ) 
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.doubleValue() , max.doubleValue() );

    int start = columnBinary.binaryStart + ( Double.BYTES * 2 + Byte.BYTES );
    int length = columnBinary.binaryLength - ( Double.BYTES * 2 + Byte.BYTES );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    int indexLength = indexMaker.calcBinarySize( columnBinary.rowCount );
    int dicLength = dicMaker.calcBinarySize( columnBinary.cardinality );

    IntBuffer indexIntBuffer =
        indexMaker.getIndexIntBuffer( binary , 0 , indexLength , order );
    PrimitiveObject[] dicArray =
        dicMaker.getDicPrimitiveArray( binary , indexLength , dicLength , order );

    for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
      int dicIndex = indexIntBuffer.get();
      if ( dicIndex != 0 ) {
        allocator.setDouble( i , dicArray[dicIndex].getDouble() );
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
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new DoubleRangeBlockIndex( min , max ) );
  }

  public class DicManager implements IDicManager {

    private final PrimitiveObject[] dicArray;

    public DicManager( final PrimitiveObject[] dicArray ) throws IOException {
      this.dicArray = dicArray;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException {
      return dicArray[index];
    }

    @Override
    public int getDicSize() throws IOException {
      return dicArray.length;
    }

  }

  public class ColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private final IDictionaryIndexMaker indexMaker;
    private final IDictionaryMaker dicMaker;
    private final ByteOrder order;

    private PrimitiveColumn column;
    private boolean isCreate;

    /**
     * Create a Column from a given ColumnBinary.
     */
    public ColumnManager(
        final ColumnBinary columnBinary ,
        final IDictionaryIndexMaker indexMaker ,
        final IDictionaryMaker dicMaker ,
        final ByteOrder order ) {
      this.columnBinary = columnBinary;
      this.indexMaker = indexMaker;
      this.dicMaker = dicMaker;
      this.order = order;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      int start = columnBinary.binaryStart + ( Double.BYTES * 2 + Byte.BYTES );
      int length = columnBinary.binaryLength - ( Double.BYTES * 2 + Byte.BYTES );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      int indexLength = indexMaker.calcBinarySize( columnBinary.rowCount );
      int dicLength = dicMaker.calcBinarySize( columnBinary.cardinality );

      IntBuffer indexIntBuffer =
          indexMaker.getIndexIntBuffer( binary , 0 , indexLength , order );
      PrimitiveObject[] dicArray =
          dicMaker.getDicPrimitiveArray( binary , indexLength , dicLength , order );

      IDicManager dicManager = new DicManager( dicArray );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager(
          new BufferDirectDictionaryLinkCellManager(
            columnBinary.columnType , dicManager , indexIntBuffer
          ) );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      if ( ! isCreate ) {
        try {
          create();
        } catch ( IOException ex ) {
          throw new UncheckedIOException( ex );
        }
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
