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
import jp.co.yahoo.yosegi.blockindex.ByteRangeBlockIndex;
import jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.yosegi.blockindex.FloatRangeBlockIndex;
import jp.co.yahoo.yosegi.blockindex.IntegerRangeBlockIndex;
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.blockindex.ShortRangeBlockIndex;
import jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.DefaultCompressor;
import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ConstantColumnBinaryMaker implements IColumnBinaryMaker {

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    throw new UnsupportedOperationException(
        "Constant binary maker not support column to binary." );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    throw new UnsupportedOperationException(
        "Constant binary maker not support column to binary." );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    int loadCount = columnBinary.rowCount;
    if ( columnBinary.loadIndex != null ) {
      loadCount = columnBinary.loadIndex.length;
    }
    return new YosegiLoaderFactory().create(
        columnBinary , loadCount );
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary , final int loadSize ) {
    int needLength = loadSize;
    if ( columnBinary.loadIndex != null ) {
      if ( columnBinary.loadIndex.length == 0 ) {
        needLength = 0;
      } else {
        needLength = columnBinary.loadIndex[columnBinary.loadIndex.length - 1];
      }
    }
    if ( needLength <= columnBinary.rowCount ) {
      return LoadType.CONST;
    } else {
      return LoadType.SEQUENTIAL;
    }
  }

  private void loadFromConst(
      final ColumnBinary columnBinary , final IConstLoader loader ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    switch ( columnBinary.columnType ) {
      case BOOLEAN:
        loader.setConstFromBoolean( wrapBuffer.get() == 1 );
        break;
      case BYTE:
        loader.setConstFromByte( wrapBuffer.get() );
        break;
      case SHORT:
        loader.setConstFromShort( wrapBuffer.getShort() );
        break;
      case INTEGER:
        loader.setConstFromInteger( wrapBuffer.getInt() );
        break;
      case LONG:
        loader.setConstFromLong( wrapBuffer.getLong() );
        break;
      case FLOAT:
        loader.setConstFromFloat( wrapBuffer.getFloat() );
        break;
      case DOUBLE:
        loader.setConstFromDouble( wrapBuffer.getDouble() );
        break;
      case STRING:
      case BYTES:
        int stringLength = wrapBuffer.getInt();
        byte[] stringBytes = new byte[stringLength];
        wrapBuffer.get( stringBytes );
        loader.setConstFromBytes( stringBytes , 0 , stringLength );
        break;
      default:
        throw new IOException( "Unknown primitive type." );
    }
  }

  abstract class SequentialLoadUtil {

    abstract void setValue( final int index , final ISequentialLoader loader ) throws IOException;

    public void set(
        final int rowCount ,
        final ISequentialLoader loader ,
        final int[] loadIndex ) throws IOException {
      if ( loadIndex == null ) {
        for ( int i = 0 ; i < loader.getLoadSize() ; i++ ) {
          if ( i < rowCount ) {
            setValue( i , loader );
          } else {
            loader.setNull(i);
          }
        }
      } else {
        for ( int i : loadIndex ) {
          if ( i < rowCount ) {
            setValue( i , loader );
          } else {
            loader.setNull(i);
          }
        }
      }
    }
  }

  private void loadFromSequential(
      final ColumnBinary columnBinary , final ISequentialLoader loader ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    SequentialLoadUtil util = null;
    switch ( columnBinary.columnType ) {
      case BOOLEAN:
        boolean booleanValue = wrapBuffer.get() == 1;
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setBoolean( index , booleanValue );
          }
        };
        break;
      case BYTE:
        byte byteValue = wrapBuffer.get();
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setByte( index , byteValue );
          }
        };
        break;
      case SHORT:
        short shortValue = wrapBuffer.getShort();
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setShort( index , shortValue );
          }
        };
        break;
      case INTEGER:
        int intValue = wrapBuffer.getInt();
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setInteger( index , intValue );
          }
        };
        break;
      case LONG:
        long longValue = wrapBuffer.getLong();
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setLong( index , longValue );
          }
        };
        break;
      case FLOAT:
        float floatValue = wrapBuffer.getFloat();
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setFloat( index , floatValue );
          }
        };
        break;
      case DOUBLE:
        double doubleValue = wrapBuffer.getDouble();
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setDouble( index , doubleValue );
          }
        };
        break;
      case STRING:
      case BYTES:
        int stringLength = wrapBuffer.getInt();
        byte[] stringBytes = new byte[stringLength];
        wrapBuffer.get( stringBytes );
        util = new SequentialLoadUtil() {
          void setValue( int index , ISequentialLoader loader ) throws IOException {
            loader.setBytes( index , stringBytes , 0 , stringLength );
          }
        };
        break;
      default:
        throw new IOException( "Unknown primitive type." );
    }
    util.set( columnBinary.rowCount , loader , columnBinary.loadIndex );
  }

  @Override
  public void load(
      final ColumnBinary columnBinary , final ILoader loader ) throws IOException {
    if ( getLoadType( columnBinary , loader.getLoadSize() ) == LoadType.CONST ) {
      if ( loader.getLoaderType() != LoadType.CONST ) {
        throw new IOException( "Loader type is not CONST." );
      }
      loadFromConst( columnBinary , (IConstLoader)loader );
    } else {
      if ( loader.getLoaderType() != LoadType.SEQUENTIAL ) {
        throw new IOException( "Loader type is not SEQUENTIAL." );
      }
      loadFromSequential( columnBinary , (ISequentialLoader)loader );
    }
    loader.finish();
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    switch ( columnBinary.columnType ) {
      case BOOLEAN:
        boolean booleanObj = wrapBuffer.get() == 1;
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setBoolean( i , booleanObj );
        }
        break;
      case BYTE:
        byte byteObj = wrapBuffer.get();
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setByte( i , byteObj );
        }
        break;
      case SHORT:
        short shortObj = wrapBuffer.getShort();
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setShort( i , shortObj );
        }
        break;
      case INTEGER:
        int intObj = wrapBuffer.getInt();
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setInteger( i , intObj );
        }
        break;
      case LONG:
        long longObj = wrapBuffer.getLong();
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setLong( i , longObj );
        }
        break;
      case FLOAT:
        float floatObj = wrapBuffer.getFloat();
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setFloat( i , floatObj );
        }
        break;
      case DOUBLE:
        double doubleObj = wrapBuffer.getDouble();
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setDouble( i , doubleObj );
        }
        break;
      case STRING:
        int stringLength = wrapBuffer.getInt();
        byte[] stringBytes = new byte[stringLength];
        wrapBuffer.get( stringBytes );
        String utf8 = new String( stringBytes , "UTF-8" );
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setString( i , utf8 );
        }
        break;
      case BYTES:
        int byteLength = wrapBuffer.getInt();
        byte[] byteBytes = new byte[byteLength];
        wrapBuffer.get( byteBytes );
        for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
          allocator.setBytes( i , byteBytes );
        }
        break;
      default:
        throw new IOException( "Unknown primitive type." );
    }
    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    switch ( columnBinary.columnType ) {
      case BYTE:
        byte byteValue = wrapBuffer.get();
        currentNode.setBlockIndex( new ByteRangeBlockIndex( byteValue , byteValue ) );
        break;
      case SHORT:
        short shortValue = wrapBuffer.getShort();
        currentNode.setBlockIndex( new ShortRangeBlockIndex( shortValue , shortValue ) );
        break;
      case INTEGER:
        int intValue = wrapBuffer.getInt();
        currentNode.setBlockIndex( new IntegerRangeBlockIndex( intValue , intValue ) );
        break;
      case LONG:
        long longValue = wrapBuffer.getLong();
        currentNode.setBlockIndex( new LongRangeBlockIndex( longValue , longValue ) );
        break;
      case FLOAT:
        float floatValue = wrapBuffer.getFloat();
        currentNode.setBlockIndex( new FloatRangeBlockIndex( floatValue , floatValue ) );
        break;
      case DOUBLE:
        double doubleValue = wrapBuffer.getDouble();
        currentNode.setBlockIndex( new DoubleRangeBlockIndex( doubleValue , doubleValue ) );
        break;
      case STRING:
        int stringLength = wrapBuffer.getInt();
        byte[] stringBytes = new byte[stringLength];
        wrapBuffer.get( stringBytes );
        String string = new String( stringBytes , 0 , stringBytes.length , "UTF-8" );
        currentNode.setBlockIndex( new StringRangeBlockIndex( string , string ) );
        break;
      default:
        currentNode.disable();
        break;
    }
  }

  /**
   * Create a constant column from the input PrimitiveObject.
   */
  public static ColumnBinary createColumnBinary(
      final PrimitiveObject value , final String columnName , final int size ) throws IOException {
    ColumnType type;
    byte[] valueBinary;
    int logicalDataSize;
    switch ( value.getPrimitiveType() ) {
      case BOOLEAN:
        type = ColumnType.BOOLEAN;
        valueBinary = new byte[Byte.BYTES];
        if ( value.getBoolean() ) {
          valueBinary[0] = 1;
        }
        logicalDataSize = Byte.BYTES * size;
        break;
      case BYTE:
        type = ColumnType.BYTE;
        valueBinary = new byte[Byte.BYTES];
        valueBinary[0] = value.getByte();
        logicalDataSize = Byte.BYTES * size;
        break;
      case SHORT:
        type = ColumnType.SHORT;
        valueBinary = new byte[Short.BYTES];
        ByteBuffer.wrap( valueBinary ).putShort( value.getShort() );
        logicalDataSize = Short.BYTES * size;
        break;
      case INTEGER:
        type = ColumnType.INTEGER;
        valueBinary = new byte[Integer.BYTES];
        ByteBuffer.wrap( valueBinary ).putInt( value.getInt() );
        logicalDataSize = Integer.BYTES * size;
        break;
      case LONG:
        type = ColumnType.LONG;
        valueBinary = new byte[Long.BYTES];
        ByteBuffer.wrap( valueBinary ).putLong( value.getLong() );
        logicalDataSize = Long.BYTES * size;
        break;
      case FLOAT:
        type = ColumnType.FLOAT;
        valueBinary = new byte[Float.BYTES];
        ByteBuffer.wrap( valueBinary ).putFloat( value.getFloat() );
        logicalDataSize = Float.BYTES * size;
        break;
      case DOUBLE:
        type = ColumnType.DOUBLE;
        valueBinary = new byte[Double.BYTES];
        ByteBuffer.wrap( valueBinary ).putDouble( value.getDouble() );
        logicalDataSize = Double.BYTES * size;
        break;
      case STRING:
        type = ColumnType.STRING;
        byte[] stringBytes = value.getBytes();
        valueBinary = new byte[Integer.BYTES + stringBytes.length];
        ByteBuffer stringWrapBuffer = ByteBuffer.wrap( valueBinary );
        stringWrapBuffer.putInt( stringBytes.length );
        stringWrapBuffer.put( stringBytes );
        logicalDataSize = ( stringBytes.length * Character.BYTES ) * size;
        break;
      case BYTES:
        type = ColumnType.BYTES;
        byte[] bytes = value.getBytes();
        valueBinary = new byte[Integer.BYTES + bytes.length];
        ByteBuffer wrapBuffer = ByteBuffer.wrap( valueBinary );
        wrapBuffer.putInt( bytes.length );
        wrapBuffer.put( bytes );
        logicalDataSize = bytes.length * size;
        break;
      default:
        throw new IOException( "Unknown primitive type." );
    }

    return new ColumnBinary(
        ConstantColumnBinaryMaker.class.getName() ,
        DefaultCompressor.class.getName() ,
        columnName ,
        type ,
        size ,
        valueBinary.length ,
        logicalDataSize ,
        1 ,
        valueBinary ,
        0 ,
        valueBinary.length ,
        null );
  }

}
