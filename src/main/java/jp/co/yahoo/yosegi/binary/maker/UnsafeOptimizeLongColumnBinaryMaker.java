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
import jp.co.yahoo.yosegi.binary.maker.index.BufferDirectSequentialNumberCellIndex;
import jp.co.yahoo.yosegi.binary.maker.index.RangeLongIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.spread.analyzer.ByteColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IntegerColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.LongColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.ShortColumnAnalizeResult;
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

public class UnsafeOptimizeLongColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Determine the type of range of difference between min and max.
   */
  public static ColumnType getDiffColumnType( final long min , final long max ) {
    long diff = max - min;
    if ( diff < 0 ) {
      return ColumnType.LONG;
    }

    if ( diff <= NumberToBinaryUtils.LONG_BYTE_MAX_LENGTH ) {
      return ColumnType.BYTE;
    } else if ( diff <= NumberToBinaryUtils.LONG_SHORT_MAX_LENGTH ) {
      return ColumnType.SHORT;
    } else if ( diff <= NumberToBinaryUtils.LONG_INT_MAX_LENGTH ) {
      return ColumnType.INTEGER;
    }

    return ColumnType.LONG;
  }

  /**
   * If they all have the same value, create a constant column.
   */
  public static PrimitiveObject createConstObject(
      final ColumnType type , final long num ) throws IOException {
    switch ( type ) {
      case BYTE:
        return new ByteObj( Long.valueOf( num ).byteValue() );
      case SHORT:
        return new ShortObj( Long.valueOf( num ).shortValue() );
      case INTEGER:
        return new IntegerObj( Long.valueOf( num ).intValue() );
      default:
        return new LongObj( num );
    }
  }

  /**
   * Create an object to create a dictionary from min and max.
   */
  public static IDictionaryMaker chooseDictionaryMaker( final long min , final long max ) {
    ColumnType diffType = getDiffColumnType( min , max );

    if ( Byte.valueOf( Byte.MIN_VALUE ).longValue() <= min
          && max <= Byte.valueOf( Byte.MAX_VALUE ).longValue() ) {
      return new ByteDictionaryMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteDictionaryMaker( min );
    } else if ( Short.valueOf( Short.MIN_VALUE ).longValue() <= min 
          && max <= Short.valueOf( Short.MAX_VALUE ).longValue() ) {
      return new ShortDictionaryMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortDictionaryMaker( min );
    } else if ( Integer.valueOf( Integer.MIN_VALUE ).longValue() <= min
          && max <= Integer.valueOf( Integer.MAX_VALUE ).longValue() ) {
      return new IntDictionaryMaker( min , max );
    } else if ( diffType == ColumnType.INTEGER ) {
      return new DiffIntDictionaryMaker( min , max );
    } else {
      return new LongDictionaryMaker( min , max );
    }
  }

  /**
   * Select the smallest numeric type in the range of Index.
   */
  public static IDictionaryIndexMaker chooseDictionaryIndexMaker(
      final int dicIndexLength ) {
    if ( dicIndexLength <= NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) {
      return new ByteDictionaryIndexMaker();
    } else if ( dicIndexLength <= NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) {
      return new ShortDictionaryIndexMaker();
    } else {
      return new IntDictionaryIndexMaker( dicIndexLength );
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
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

  }

  public static class ByteDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Byte.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Byte.BYTES * dicSize;
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
        wrapBuffer.putByte( obj.getByte() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      wrapBuffer.getByte();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new ByteObj( wrapBuffer.getByte() );
      }

      return result;
    }

  }

  public static class DiffByteDictionaryMaker implements IDictionaryMaker {

    private final long min;

    public DiffByteDictionaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Byte.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Byte.BYTES * dicSize;
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
        wrapBuffer.putByte( (byte)( obj.getLong() - min ) );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      wrapBuffer.getByte();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] =
          new LongObj( NumberToBinaryUtils.getUnsignedByteToLong( wrapBuffer.getByte() ) + min );
      }

      return result;
    }

  }

  public static class ShortDictionaryMaker implements IDictionaryMaker {

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Short.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Short.BYTES * dicSize;
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
        wrapBuffer.putShort( obj.getShort() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      wrapBuffer.getShort();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new ShortObj( wrapBuffer.getShort() );
      }

      return result;
    }

  }

  public static class DiffShortDictionaryMaker implements IDictionaryMaker {

    private final long min;

    public DiffShortDictionaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Short.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return Short.BYTES * dicSize;
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
        wrapBuffer.putShort( (short)( obj.getLong() - min ) );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      wrapBuffer.getShort();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new LongObj(
            NumberToBinaryUtils.getUnsignedShortToLong( wrapBuffer.getShort() ) + min );
      }

      return result;
    }

  }

  public static class IntDictionaryMaker implements IDictionaryMaker {

    private final NumberToBinaryUtils.IIntConverter converter;

    public IntDictionaryMaker( final long min , final long max ) {
      converter = NumberToBinaryUtils.getIntConverter( (int)min , (int)max );
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Integer.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return converter.calcBinarySize( dicSize );
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          converter.toWriteSuppoter( objList.size() , buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putInt( obj.getInt() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      wrapBuffer.getInt();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new IntegerObj( wrapBuffer.getInt() );
      }

      return result;
    }

  }

  public static class DiffIntDictionaryMaker implements IDictionaryMaker {

    private final long min;
    private final NumberToBinaryUtils.IIntConverter converter;

    /**
     * Take the difference between min and max and create a dictionary within Int.
     */
    public DiffIntDictionaryMaker( final long min , final long max ) {
      this.min = min;
      int diff = (int)(max - min);
      converter = NumberToBinaryUtils.getIntConverter( 0 , diff );
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Integer.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return converter.calcBinarySize( dicSize );
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          converter.toWriteSuppoter( objList.size() , buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putInt( (int)( obj.getLong() - min ) );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      wrapBuffer.getInt();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] =
            new LongObj( NumberToBinaryUtils.getUnsignedIntToLong( wrapBuffer.getInt() ) + min );
      }

      return result;
    }

  }

  public static class LongDictionaryMaker implements IDictionaryMaker {

    private final NumberToBinaryUtils.ILongConverter converter;

    public LongDictionaryMaker( final long min , final long max ) {
      converter = NumberToBinaryUtils.getLongConverter( min , max );
    }

    @Override
    public int getLogicalSize( final int indexLength ) {
      return Long.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ) {
      return converter.calcBinarySize( dicSize );
    }

    @Override
    public void create(
        final List<PrimitiveObject> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          converter.toWriteSuppoter( objList.size() , buffer , start , length );
      for ( PrimitiveObject obj : objList ) {
        wrapBuffer.putLong( obj.getLong() );
      }
    }

    @Override
    public PrimitiveObject[] getDicPrimitiveArray(
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[size];
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      wrapBuffer.getLong();
      for ( int i = 1 ; i < size ; i++ ) {
        result[i] = new LongObj( wrapBuffer.getLong() );
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

    private final NumberToBinaryUtils.IIntConverter converter;

    public IntDictionaryIndexMaker( final int dicSize ) {
      converter = NumberToBinaryUtils.getIntConverter( 0 , dicSize );
    }

    @Override
    public int calcBinarySize( final int indexLength ) {
      return converter.calcBinarySize( indexLength );
    }

    @Override
    public void create(
        final int[] dicIndexArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer = 
          converter.toWriteSuppoter( dicIndexArray.length , buffer , start , length );
      for ( int index : dicIndexArray ) {
        wrapBuffer.putInt( index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      int size = length / Integer.BYTES;
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      IntBuffer result = IntBuffer.allocate( size );
      for ( int i = 0 ; i < size ; i++ ) {
        result.put( (int)( wrapBuffer.getInt() ) );
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
    Map<Long,Integer> dicMap = new HashMap<Long,Integer>();
    List<PrimitiveObject> dicList = new ArrayList<PrimitiveObject>();
    int[] indexArray = new int[column.size()];

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( new LongObj( 0L ) );

    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      Long target = null;
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
      } else {
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        target = Long.valueOf( primitiveObj.getLong() );
      }
      if ( ! dicMap.containsKey( target ) ) {
        if ( 0 < min.compareTo( target ) ) {
          min = Long.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Long.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicList.add( primitiveObj );
      }
      indexArray[i] = dicMap.get( target );
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          createConstObject( column.getColumnType() , min ) ,
          column.getColumnName() ,
          column.size() );
    }

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( indexArray.length );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.longValue() , max.longValue() );
    ByteOrder order = ByteOrder.nativeOrder();

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

    byte[] binary = new byte[ Long.BYTES * 2 + Byte.BYTES + compressBinary.length ];

    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( min );
    wrapBuffer.putLong( max );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() , binary.length ,
        dicMaker.getLogicalSize( rowCount ) ,
        dicList.size() ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    long min;
    long max;
    switch ( analizeResult.getColumnType() ) {
      case BYTE:
        min = (long)( (ByteColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ByteColumnAnalizeResult) analizeResult ).getMax();
        break;
      case SHORT:
        min = (long)( (ShortColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ShortColumnAnalizeResult) analizeResult ).getMax();
        break;
      case INTEGER:
        min = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMax();
        break;
      case LONG:
        min = ( (LongColumnAnalizeResult) analizeResult ).getMin();
        max = ( (LongColumnAnalizeResult) analizeResult ).getMax();
        break;
      default:
        min = Long.MIN_VALUE;
        max = Long.MAX_VALUE;
        break;
    }
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
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.longValue() , max.longValue() );
    return new HeaderIndexLazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new ColumnManager(
          columnBinary ,
          indexMaker ,
          dicMaker ,
          order
        ) , 
        new RangeLongIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.longValue() , max.longValue() );

    int start = columnBinary.binaryStart + ( Long.BYTES * 2 + Byte.BYTES );
    int length = columnBinary.binaryLength - ( Long.BYTES * 2 + Byte.BYTES );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    int indexLength = indexMaker.calcBinarySize( columnBinary.rowCount );
    int dicLength = dicMaker.calcBinarySize( columnBinary.cardinality );

    IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer( binary , 0 , indexLength , order );
    PrimitiveObject[] dicArray = dicMaker.getDicPrimitiveArray(
        columnBinary.cardinality , binary , indexLength , dicLength , order );

    int loopCount = indexIntBuffer.capacity();
    for ( int i = 0 ; i < loopCount ; i++ ) {
      int dicIndex = indexIntBuffer.get();
      if ( dicIndex == 0 ) {
        allocator.setNull( i );
      } else {
        allocator.setLong( i , dicArray[dicIndex].getLong() );
      }
    }
    allocator.setValueCount( loopCount );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( 
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
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
      int start = columnBinary.binaryStart + ( Long.BYTES * 2 + Byte.BYTES );
      int length = columnBinary.binaryLength - ( Long.BYTES * 2 + Byte.BYTES );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      int indexLength = indexMaker.calcBinarySize( columnBinary.rowCount );
      int dicLength = dicMaker.calcBinarySize( columnBinary.cardinality );

      IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer( binary , 0 , indexLength , order );
      PrimitiveObject[] dicArray = dicMaker.getDicPrimitiveArray(
          columnBinary.cardinality , binary , indexLength , dicLength , order );

      IDicManager dicManager = new DicManager( dicArray );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager(
          columnBinary.columnType , dicManager , indexIntBuffer ) );
      column.setIndex( new BufferDirectSequentialNumberCellIndex(
          columnBinary.columnType , dicManager , indexIntBuffer ) );

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
