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
import jp.co.yahoo.yosegi.binary.maker.index.BufferDirectSequentialStringCellIndex;
import jp.co.yahoo.yosegi.binary.maker.index.RangeStringIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.DataType;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.Utf8BytesLinkObj;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.StringColumnAnalizeResult;
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

public class UnsafeOptimizeStringColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Determine the type of range of difference between min and max.
   */
  public static ColumnType getDiffColumnType( final int min , final int max ) {
    int diff = max - min;
    if ( diff < 0 ) {
      return ColumnType.INTEGER;
    }

    if ( diff <= NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) {
      return ColumnType.BYTE;
    } else if ( diff <= NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) {
      return ColumnType.SHORT;
    }

    return ColumnType.INTEGER;
  }

  /**
   * Create an object to save with minimum type from string length min and max.
   */
  public static ILengthMaker chooseLengthMaker( final int min , final int max ) {
    if ( min == max ) {
      return new FixedLengthMaker( min );
    }

    ColumnType diffType = getDiffColumnType( min , max );
    if ( max <= Byte.valueOf( Byte.MAX_VALUE ).intValue() ) {
      return new ByteLengthMaker();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffByteLengthMaker( min );
    } else if ( max <= Short.valueOf( Short.MAX_VALUE ).intValue() ) {
      return new ShortLengthMaker();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffShortLengthMaker( min );
    } else {
      return new IntLengthMaker();
    }
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
      return new IntDictionaryIndexMaker( 0 , dicIndexLength );
    }
  }

  public interface ILengthMaker {

    int calcBinarySize( final int columnSize );

    void create(
        List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException;

    int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException;

  }

  public static class FixedLengthMaker implements ILengthMaker {

    private final int min;

    public FixedLengthMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return 0;
    }

    @Override
    public void create(
        final List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      return;
    }

    @Override
    public int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException {
      int[] result = new int[size];
      for ( int i = 0 ; i < result.length ; i++ ) {
        result[i] = min;
      }
      return result;
    }

  }

  public static class ByteLengthMaker implements ILengthMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( byte[] obj : objList ) {
        wrapBuffer.putByte( (byte)obj.length );
      }
    }

    @Override
    public int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getByte();
      }
      return result;
    }

  }

  public static class DiffByteLengthMaker implements ILengthMaker {

    private final int min;

    public DiffByteLengthMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( byte[] obj : objList ) {
        wrapBuffer.putByte( (byte)( obj.length - min ) );
      }
    }

    @Override
    public int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = NumberToBinaryUtils.getUnsignedByteToInt( wrapBuffer.getByte() ) + min;
      }
      return result;
    }

  }

  public static class ShortLengthMaker implements ILengthMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( byte[] obj : objList ) {
        wrapBuffer.putShort( (short)obj.length );
      }
    }

    @Override
    public int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getShort();
      }
      return result;
    }

  }

  public static class DiffShortLengthMaker implements ILengthMaker {

    private final int min;

    public DiffShortLengthMaker( final int min ) {
      this.min = min;
    }

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Short.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( byte[] obj : objList ) {
        wrapBuffer.putShort( (short)( obj.length - min ) );
      }
    }

    @Override
    public int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = NumberToBinaryUtils.getUnsignedShortToInt( wrapBuffer.getShort() ) + min;
      }
      return result;
    }

  }

  public static class IntLengthMaker implements ILengthMaker {

    @Override
    public int calcBinarySize( final int columnSize ) {
      return Integer.BYTES * columnSize;
    }

    @Override
    public void create(
        final List<byte[]> objList ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IWriteSupporter wrapBuffer =
          ByteBufferSupporterFactory.createWriteSupporter( buffer , start , length , order );
      for ( byte[] obj : objList ) {
        wrapBuffer.putInt( obj.length );
      }
    }

    @Override
    public int[] getLengthArray(
        final byte[] buffer ,
        final int start ,
        final int length ,
        final int size ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer =
          ByteBufferSupporterFactory.createReadSupporter( buffer , start , length , order );
      int[] result = new int[size];
      for ( int i = 0 ; i < size ; i++ ) {
        result[i] = wrapBuffer.getInt();
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
        final int size ,
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
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
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
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
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

    public IntDictionaryIndexMaker( final int min , final int max ) {
      converter = NumberToBinaryUtils.getIntConverter( min , max );
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
        final int size ,
        final byte[] buffer ,
        final int start ,
        final int length ,
        final ByteOrder order ) throws IOException {
      IReadSupporter wrapBuffer = converter.toReadSupporter( buffer , start , length );
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
      final IColumn column ) throws IOException {
    if ( column.size() == 0 ) {
      return new UnsupportedColumnBinaryMaker()
          .toBinary( commonConfig , currentConfigNode , column );
    }

    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    Map<String,Integer> dicMap = new HashMap<String,Integer>();
    int[] indexArray = new int[ column.size() ];
    List<byte[]> stringList = new ArrayList<byte[]>();
    dicMap.put( null , 0 );
    stringList.add( new byte[0] );

    int totalLength = 0;
    int logicalDataLength = 0;
    boolean hasNull = false;
    String min = null;
    String max = "";
    int minLength = Integer.MAX_VALUE;
    int maxLength = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        indexArray[i] = 0;
        hasNull = true;
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      String strObj = byteCell.getRow().getString();
      if ( strObj == null ) {
        indexArray[i] = 0;
        hasNull = true;
        continue;
      }
      if ( ! dicMap.containsKey( strObj ) ) {
        dicMap.put( strObj , stringList.size() );
        byte[] obj = strObj.getBytes( "UTF-8" );
        stringList.add( obj );
        if ( maxLength < obj.length ) {
          maxLength = obj.length;
        }
        if ( obj.length < minLength ) {
          minLength = obj.length;
        }
        totalLength += obj.length;
        if ( max.compareTo( strObj ) < 0 ) {
          max = strObj;
        }
        if ( min == null || 0 < min.compareTo( strObj ) ) {
          min = strObj;
        }
      }
      int dicIndex = dicMap.get( strObj );
      indexArray[i] = dicIndex;
      logicalDataLength += Integer.BYTES + stringList.get( dicIndex ).length;
    }

    if ( ! hasNull && min.equals( max ) ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new StringObj( min ) , column.getColumnName() , column.size() );
    }

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( indexArray.length );
    ILengthMaker lengthMaker = chooseLengthMaker( minLength , maxLength );

    int indexBinaryLength = indexMaker.calcBinarySize( indexArray.length );
    int lengthBinaryLength = lengthMaker.calcBinarySize( stringList.size() );

    ByteOrder order = ByteOrder.nativeOrder();
    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

    int binaryLength =
        Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength + lengthBinaryLength + totalLength;
    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , binaryRaw.length );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.putInt( minLength );
    wrapBuffer.putInt( maxLength );

    indexMaker.create(
        indexArray , binaryRaw , Byte.BYTES + Integer.BYTES * 2 , indexBinaryLength , order );

    lengthMaker.create(
        stringList ,
        binaryRaw ,
        Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength ,
        lengthBinaryLength ,
        order );
    wrapBuffer.position( Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength + lengthBinaryLength );
    for ( byte[] obj : stringList ) {
      wrapBuffer.put( obj );
    }
    byte[] compressBinaryRaw =
        currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length , DataType.TEXT );

    int minCharLength = Character.BYTES * min.length();
    int maxCharLength = Character.BYTES * max.length();
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    byte[] binary = new byte[headerSize + compressBinaryRaw.length];
    ByteBuffer binaryWrapBuffer = ByteBuffer.wrap( binary );
    binaryWrapBuffer.putInt( minCharLength );
    binaryWrapBuffer.asCharBuffer().put( min );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + minCharLength );
    binaryWrapBuffer.putInt( maxCharLength );
    binaryWrapBuffer.asCharBuffer().put( max );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + maxCharLength );
    binaryWrapBuffer.put( compressBinaryRaw );
    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.STRING ,
        column.size() ,
        binaryRaw.length ,
        logicalDataLength ,
        stringList.size() ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    boolean hasNull = analizeResult.getNullCount() != 0;
    if ( ! hasNull && analizeResult.getUniqCount() == 1 ) {
      return stringAnalizeResult.getUniqUtf8ByteSize();
    }
    IDictionaryIndexMaker indexMaker =
        chooseDictionaryIndexMaker( stringAnalizeResult.getColumnSize() );
    ILengthMaker lengthMaker = chooseLengthMaker(
        stringAnalizeResult.getMinUtf8Bytes() , stringAnalizeResult.getMaxUtf8Bytes() );

    int indexBinaryLength = indexMaker.calcBinarySize( stringAnalizeResult.getColumnSize() );
    int lengthBinaryLength = lengthMaker.calcBinarySize( stringAnalizeResult.getUniqCount() );

    return Integer.BYTES * 2
        + indexBinaryLength
        + lengthBinaryLength
        + stringAnalizeResult.getUniqUtf8ByteSize();
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    int headerSize = Integer.BYTES + minLength + Integer.BYTES + maxLength;
    return new HeaderIndexLazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new StringColumnManager(
          columnBinary ,
          columnBinary.binaryStart + headerSize ,
          columnBinary.binaryLength - headerSize ) ,
        new RangeStringIndex( min , max ) );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer rawBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minBinaryLength = rawBuffer.getInt();
    rawBuffer.position( rawBuffer.position() + minBinaryLength );

    int maxBinaryLength = rawBuffer.getInt();
    rawBuffer.position( rawBuffer.position() + maxBinaryLength );

    int headerSize = Integer.BYTES + minBinaryLength + Integer.BYTES + maxBinaryLength;

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress(
        columnBinary.binary ,
        columnBinary.binaryStart + headerSize ,
        columnBinary.binaryLength - headerSize );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    ILengthMaker lengthMaker = chooseLengthMaker( minLength , maxLength );

    int indexBinaryLength = indexMaker.calcBinarySize( columnBinary.rowCount );
    int lengthBinaryLength = lengthMaker.calcBinarySize( columnBinary.cardinality );

    IntBuffer indexBuffer = indexMaker.getIndexIntBuffer(
        columnBinary.rowCount ,
        binary ,
        Byte.BYTES + Integer.BYTES * 2 ,
        indexBinaryLength ,
        order );
    int[] lengthArray = lengthMaker.getLengthArray(
        binary ,
        Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength ,
        lengthBinaryLength ,
        columnBinary.cardinality ,
        order );

    int currentStart = Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength + lengthBinaryLength;

    Utf8BytesLinkObj[] dicArray = new Utf8BytesLinkObj[ lengthArray.length ];
    for ( int i = 1 ; i < dicArray.length ; i++ ) {
      dicArray[i] = new Utf8BytesLinkObj( binary , currentStart , lengthArray[i] );
      currentStart += lengthArray[i];
    }

    for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
      int index = indexBuffer.get();
      if ( index == 0 ) {
        allocator.setNull( i );
      } else {
        allocator.setBytes(
            i ,
            dicArray[index].getLinkBytes() ,
            dicArray[index].getStart() ,
            dicArray[index].getLength() );
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
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new StringRangeBlockIndex( min , max ) );
  }

  public class RangeStringDicManager implements IDicManager {

    private final PrimitiveObject[] dicArray;

    public RangeStringDicManager( final PrimitiveObject[] dicArray ) {
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

  public class StringColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    /**
     * Create a Column from a given ColumnBinary.
     */
    public StringColumnManager(
        final ColumnBinary columnBinary ,
        final int binaryStart ,
        final int binaryLength ) throws IOException {
      this.columnBinary = columnBinary;
      this.binaryStart = binaryStart;
      this.binaryLength = binaryLength;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , binaryStart , binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
      ByteOrder order = wrapBuffer.get() == (byte)0
          ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int minLength = wrapBuffer.getInt();
      int maxLength = wrapBuffer.getInt();

      IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
      ILengthMaker lengthMaker = chooseLengthMaker( minLength , maxLength );

      int indexBinaryLength = indexMaker.calcBinarySize( columnBinary.rowCount );
      int lengthBinaryLength = lengthMaker.calcBinarySize( columnBinary.cardinality );

      IntBuffer indexBuffer = indexMaker.getIndexIntBuffer(
          columnBinary.rowCount ,
          binary ,
          Byte.BYTES + Integer.BYTES * 2 ,
          indexBinaryLength , order );

      int[] lengthArray = lengthMaker.getLengthArray(
          binary ,
          Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength ,
          lengthBinaryLength ,
          columnBinary.cardinality ,
          order );

      int currentStart = Byte.BYTES + Integer.BYTES * 2 + indexBinaryLength + lengthBinaryLength;

      PrimitiveObject[] dicArray = new PrimitiveObject[ lengthArray.length ];
      for ( int i = 1 ; i < dicArray.length ; i++ ) {
        dicArray[i] = new Utf8BytesLinkObj( binary , currentStart , lengthArray[i] );
        currentStart += lengthArray[i];
      }

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      IDicManager dicManager = new RangeStringDicManager( dicArray );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager(
          columnBinary.columnType , dicManager , indexBuffer ) );
      column.setIndex( new BufferDirectSequentialStringCellIndex( dicManager , indexBuffer ) );

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
