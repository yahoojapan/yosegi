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
import java.util.ArrayList;
import java.util.List;

public class UnsafeOptimizeDumpLongColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Calculate logical data size from column size and type.
   */
  public static int getLogicalSize( final int columnSize , final ColumnType columnType ) {
    int byteSize = Long.BYTES;
    switch ( columnType ) {
      case BYTE:
        byteSize = Byte.BYTES;
        break;
      case SHORT:
        byteSize = Short.BYTES;
        break;
      case INTEGER:
        byteSize = Integer.BYTES;
        break;
      default:
        byteSize = Long.BYTES;
        break;
    }
    return columnSize * byteSize;
  }

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
   * Create a numeric column from the entered type. 
   */
  public static PrimitiveObject createConstObjectFromNum(
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
   * Create an IBinaryMaker to save from min and max with the smallest byte.
   */
  public static IBinaryMaker chooseBinaryMaker( final long min , final long max ) {
    ColumnType diffType = getDiffColumnType( min , max );

    if ( Byte.valueOf( Byte.MIN_VALUE ).longValue() <= min
        && max <= Byte.valueOf( Byte.MAX_VALUE ).longValue() ) {
      return new ByteBinaryMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteBinaryMaker( min );
    } else if ( Short.valueOf( Short.MIN_VALUE ).longValue() <= min
        && max <= Short.valueOf( Short.MAX_VALUE ).longValue() ) {
      return new ShortBinaryMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortBinaryMaker( min );
    } else if ( Integer.valueOf( Integer.MIN_VALUE ).longValue() <= min
        && max <= Integer.valueOf( Integer.MAX_VALUE ).longValue() ) {
      return new IntBinaryMaker( min , max );
    } else if ( diffType == ColumnType.INTEGER ) {
      return new DiffIntBinaryMaker( min , max );
    } else {
      return new LongBinaryMaker( min , max );
    }
  }

  public interface IBinaryMaker {

    int calcBinarySize( final int columnSize );

    void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException;

    PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException;

    void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException;

  }

  public static class ByteBinaryMaker implements IBinaryMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        wrapBuffer.putByte( Long.valueOf( longArray[i] ).byteValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < columnSize; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new ByteObj( wrapBuffer.getByte() );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == (byte)0 ) {
          allocator.setByte( i , wrapBuffer.getByte() );
        } else {
          allocator.setNull( i );
        }
      }
    }

  }

  public static class DiffByteBinaryMaker implements IBinaryMaker {

    private final long min;

    public DiffByteBinaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        byte castValue = (byte)( longArray[i] - min );
        wrapBuffer.putByte( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < columnSize ; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new LongObj(
              NumberToBinaryUtils.getUnsignedByteToLong( wrapBuffer.getByte() ) + min );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == 0 ) {
          allocator.setLong(
              i , NumberToBinaryUtils.getUnsignedByteToLong( wrapBuffer.getByte() ) + min );
        } else {
          allocator.setNull( i );
        }
      }
    }

  }

  public static class ShortBinaryMaker implements IBinaryMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        wrapBuffer.putShort( Long.valueOf( longArray[i] ).shortValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < columnSize ; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new ShortObj( wrapBuffer.getShort() );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,  
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == 0) {
          allocator.setShort( i , wrapBuffer.getShort() );
        } else {
          allocator.setNull( i );
        }
      }
    }

  }

  public static class DiffShortBinaryMaker implements IBinaryMaker {

    private final long min;

    public DiffShortBinaryMaker( final long min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        short castValue = (short)( longArray[i] - min );
        wrapBuffer.putShort( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < columnSize ; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new LongObj(
              NumberToBinaryUtils.getUnsignedShortToLong( wrapBuffer.getShort() ) + min );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == 0 ) {
          allocator.setLong(
              i , NumberToBinaryUtils.getUnsignedShortToLong( wrapBuffer.getShort() ) + min );
        } else {
          allocator.setNull( i );
        }
      }
    }

  }

  public static class IntBinaryMaker implements IBinaryMaker {

    private final NumberToBinaryUtils.IIntConverter converter;

    public IntBinaryMaker( final long min , final long  max ) {
      converter = NumberToBinaryUtils.getIntConverter( (int)min , (int)max );
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return converter.calcBinarySize( columnSize );
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter supporter =
          converter.toWriteSuppoter( rowCount , buffer , start , length );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        supporter.putInt( (int)longArray[i] );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      for ( int i = 0 ; i < columnSize ; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new IntegerObj( wrapBuffer.getInt() );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == 0 ) {
          allocator.setInteger( i , wrapBuffer.getInt() );
        } else {
          allocator.setNull( i );
        }
      }
    }

  }

  public static class DiffIntBinaryMaker implements IBinaryMaker {

    private final long min;
    private final NumberToBinaryUtils.IIntConverter converter;

    public DiffIntBinaryMaker( final long min , final long  max ) {
      converter = NumberToBinaryUtils.getIntConverter( (int)min , (int)max );
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return converter.calcBinarySize( columnSize );
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter supporter =
          converter.toWriteSuppoter( rowCount , buffer , start , length );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        int castValue = (int)( longArray[i] - min );
        supporter.putInt( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      for ( int i = 0 ; i < columnSize ; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new LongObj(
              NumberToBinaryUtils.getUnsignedIntToLong( wrapBuffer.getInt() ) + min );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == 0 ) {
          allocator.setLong(
              i , NumberToBinaryUtils.getUnsignedIntToLong( wrapBuffer.getInt() ) + min );
        } else {
          allocator.setNull( i );
        }
      }
    }

  }

  public static class LongBinaryMaker implements IBinaryMaker {

    private final NumberToBinaryUtils.ILongConverter converter;

    public LongBinaryMaker( final long min , final long max ) {
      converter = NumberToBinaryUtils.getLongConverter( min , max );
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return converter.calcBinarySize( columnSize );
    }

    @Override
    public void create(
        final long[] longArray ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ,
        final int rowCount ) throws IOException {
      IWriteSupporter wrapBuffer =
          converter.toWriteSuppoter( rowCount , buffer , start , length );
      for ( int i = 0 ; i < rowCount ; i++ ) {
        wrapBuffer.putLong( longArray[i] );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int columnSize ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      PrimitiveObject[] result = new PrimitiveObject[columnSize];
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      for ( int i = 0 ; i < columnSize ; i++ ) {
        if ( ! hasNull || buffer[i] == (byte)0 ) {
          result[i] = new LongObj( wrapBuffer.getLong() );
        }
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ,
        final byte[] isNullArray ,
        final int size ,
        final boolean hasNull ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
      for ( int i = 0 ; i < size ; i++ ) {
        if ( ! hasNull || isNullArray[i] == 0 ) {
          allocator.setLong( i , wrapBuffer.getLong() );
        } else {
          allocator.setNull( i );
        }
      }
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
    long[] valueArray = new long[column.size()];
    byte[] isNullArray = new byte[column.size()];

    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      if ( cell.getType() == ColumnType.NULL ) {
        hasNull = true;
        isNullArray[i] = 1;
      } else {
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        valueArray[rowCount] = primitiveObj.getLong();
        if ( 0 < min.compareTo( valueArray[rowCount] ) ) {
          min = Long.valueOf( valueArray[rowCount] );
        }
        if ( max.compareTo( valueArray[rowCount] ) < 0 ) {
          max = Long.valueOf( valueArray[rowCount] );
        }
        rowCount++;
      }
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          createConstObjectFromNum( column.getColumnType() , min ) ,
          column.getColumnName() ,
          column.size() );
    }

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );
    ByteOrder order = ByteOrder.nativeOrder();

    int nullBinaryLength = 0;
    if ( hasNull ) {
      nullBinaryLength = isNullArray.length;
    }
    int valueLength = binaryMaker.calcBinarySize( rowCount );

    byte[] binaryRaw = new byte[ nullBinaryLength + valueLength ];
    if ( hasNull ) {
      ByteBuffer compressBinaryBuffer = ByteBuffer.wrap( binaryRaw , 0 , nullBinaryLength );
      compressBinaryBuffer.put( isNullArray );
    }
    binaryMaker.create(
        valueArray , binaryRaw , nullBinaryLength , valueLength , order , rowCount );

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary =
        new byte[ Long.BYTES * 2 + Byte.BYTES * 2 + Integer.BYTES + compressBinary.length ];

    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( min );
    wrapBuffer.putLong( max );
    wrapBuffer.put( hasNull ? (byte)1 : (byte)0 );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.putInt( rowCount );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binary.length ,
        getLogicalSize( rowCount , column.getColumnType() ) ,
        -1 ,
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
    IBinaryMaker binaryMaker = chooseBinaryMaker( min , max );
    int nullBinaryLength = analizeResult.getColumnSize();
    int valueLength = binaryMaker.calcBinarySize( analizeResult.getColumnSize() );
    return nullBinaryLength + valueLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    boolean hasNull = wrapBuffer.get() == (byte)0 ? false : true;
    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int rowCount = wrapBuffer.getInt();

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );
    return new LazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new ColumnManager(
          columnBinary ,
          binaryMaker ,
          hasNull ,
          order ,
          rowCount
        ) 
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    boolean hasNull = wrapBuffer.get() == (byte)0 ? false : true;
    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int rowCount = wrapBuffer.getInt();

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );

    int start = columnBinary.binaryStart + ( Long.BYTES * 2 + Byte.BYTES * 2 + Integer.BYTES );
    int length = columnBinary.binaryLength - ( Long.BYTES * 2 + Byte.BYTES * 2 + Integer.BYTES );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    int isNullLength = 0;
    if ( hasNull ) {
      isNullLength = columnBinary.rowCount;
    }
    int binaryLength = binaryMaker.calcBinarySize( rowCount );

    binaryMaker.loadInMemoryStorage(
        binary ,
        isNullLength ,
        binaryLength ,
        allocator ,
        binary ,
        columnBinary.rowCount ,
        hasNull ,
        order );

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
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }

  public class DicManager implements IDicManager {

    private final PrimitiveObject[] dicArray;
    private final byte[] nullArray;
    private final boolean hasNull;

    /**
     * Access in dictionary format.
     */
    public DicManager(
        final PrimitiveObject[] dicArray ,
        final byte[] nullArray ,
        final boolean hasNull ) throws IOException {
      this.dicArray = dicArray;
      this.nullArray = nullArray;
      this.hasNull = hasNull;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException {
      if ( ! hasNull || nullArray[index] == 0 ) {
        return dicArray[index];
      } else {
        return null;
      }
    }

    @Override
    public int getDicSize() throws IOException {
      return dicArray.length;
    }

  }

  public class ColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private final IBinaryMaker binaryMaker;
    private final boolean hasNull;
    private final ByteOrder order;
    private final int rowCount;

    private PrimitiveColumn column;
    private boolean isCreate;

    /**
     * Create a Column from a given ColumnBinary.
     */
    public ColumnManager(
        final ColumnBinary columnBinary ,
        final IBinaryMaker binaryMaker ,
        final boolean hasNull ,
        final ByteOrder order ,
        final int rowCount ) {
      this.columnBinary = columnBinary;
      this.binaryMaker = binaryMaker;
      this.hasNull = hasNull;
      this.order = order;
      this.rowCount = rowCount;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      int start = columnBinary.binaryStart + ( Long.BYTES * 2 + Byte.BYTES * 2 + Integer.BYTES );
      int length = columnBinary.binaryLength - ( Long.BYTES * 2 + Byte.BYTES * 2 + Integer.BYTES );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      int isNullLength = 0;
      if ( hasNull ) {
        isNullLength = columnBinary.rowCount;
      }
      int binaryLength = binaryMaker.calcBinarySize( rowCount );

      PrimitiveObject[] dicArray = binaryMaker.getPrimitiveArray(
          binary ,
          isNullLength ,
          binaryLength ,
          columnBinary.rowCount ,
          hasNull ,
          order );

      IDicManager dicManager = new DicManager( dicArray , binary , hasNull );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectCellManager(
          columnBinary.columnType , dicManager , columnBinary.rowCount ) );

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
