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

package jp.co.yahoo.yosegi.binary;

import jp.co.yahoo.yosegi.compressor.CompressorNameShortCut;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ColumnTypeFactory;
import jp.co.yahoo.yosegi.stats.ColumnStats;
import jp.co.yahoo.yosegi.stats.SummaryStats;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is a variable that holds the serialized data of the column.
 * Its role is to get statistics, get binary data size,
 * serialize and deserialize when saving to a block.
 *
 * <p>Does not allow NULL except columnBinaryList.
 * If columnBinaryList is NULL, it means Primitive type.
 * 
 * <p>This object is generated from a class that implements the
 * {@link jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker} interface.
 * Since the serialized byte array is a reference,
 * there is a risk of being rewritten by other classes.
 * The reason for creating a new byte array and not copying
 * it is because of poor performance.
 *
 * <p>Multi-thread access is not assumed because variables are referenced directly.
 *
 * <p>The binary layout is shown below.
 *
 * <ul>
 * <li>makerClassNameCharLength int
 * <li>makerClassName varchar
 * <li>compressorClassNameCharLength int
 * <li>compressorClassName varchar
 * <li>columnNameCharLength int 
 * <li>columnName varchar
 * <li>columnType byte
 * <li>rowCount int
 * <li>logicalDataSize int
 * <li>cardinality int
 * <li>binaryStart int
 * <li>binaryLength int
 * </ul>
 *
 * <p>The fixed length is Integer.BYTES * 9 + Byte.BYTES. 
 * This serialized data contains no data. This is because when creating a block,
 * meta and data are separated and copied into a byte array.
 */
public class ColumnBinary {

  /** The fixed length is Integer.BYTES * 9 + Byte.BYTES. */
  private static final int FIXED_BINARY_SIZE = Integer.BYTES * 9 + Byte.BYTES;

  public final String makerClassName;
  public final String compressorClassName;
  public final String columnName;
  public final ColumnType columnType;
  public final int rowCount;
  public final int rawDataSize;
  public final int logicalDataSize;
  public final int cardinality;

  public int binaryStart;
  public int binaryLength;
  public byte[] binary;

  public List<ColumnBinary> columnBinaryList;

  public int[] loadIndex;

  /**
   * Create an object initialized with argument values.
   * There is a risk that the value set at initialization is rewritten
   * because direct reference is allowed.
   *
   * @param makerClassName the class name that has the interface of IColumnBinaryMaker.
   * @param compressorClassName compression class name used for serialization.
   * @param columnName column name.
   * @param columnType column type.
   * @param rowCount the raw count for this column. Does not include NULL.
   * @param rawDataSize data size after serialization.
   * @param logicalDataSize logical data size of this column.
   * @param cardinality cardinality of this column. If not calculated, it is -1.
   * @param binary byte array with column serialized.
   * @param binaryStart binary start position.
   * @param binaryLength binary length.
   */
  public ColumnBinary(
      final String makerClassName ,
      final String compressorClassName ,
      final String columnName ,
      final ColumnType columnType ,
      final int rowCount ,
      final int rawDataSize ,
      final int logicalDataSize ,
      final int cardinality ,
      final byte[] binary ,
      final int binaryStart ,
      final int binaryLength ,
      final List<ColumnBinary> columnBinaryList ) {
    this.makerClassName = makerClassName;
    this.compressorClassName = compressorClassName;
    this.columnName = columnName;
    this.columnType = columnType;
    this.rowCount = rowCount;
    this.rawDataSize = rawDataSize;
    this.logicalDataSize = logicalDataSize;
    this.cardinality = cardinality;
    this.binaryStart = binaryStart;
    this.binaryLength = binaryLength;
    this.binary = binary;
    this.columnBinaryList = columnBinaryList;
  }

  /**
   * Returns the size of the data.
   * It does not include the binary size of the meta.
   * If List is not NULL, calculate the sum.
   * There is a {@link #getMetaSize ()} that calculates the metasize,
   * but does not include the size of the data.
   *
   * @return Size of byte array when saving to block.
   *         If children are included, add them up.
   */
  public int binarySize() {
    int length = binaryLength;

    if ( columnBinaryList != null ) {
      for ( ColumnBinary child : columnBinaryList ) {
        length += child.binarySize();
      }
    }
    return length;
  }

  /**
   * Returns the binary size of this object.
   * The difference from {@link #binarySize ()} is that it does not include children.
   *
   * @return Size of byte array when saving to block.
   *         If children are included, add them up.
   */
  public int getMetaSize() {
    return
        ( ColumnBinaryMakerNameShortCut.getShortCutName( makerClassName ).length()
          * Character.BYTES )
        + ( CompressorNameShortCut.getShortCutName( compressorClassName ).length()
          * Character.BYTES )
        + ( columnName.length() * Character.BYTES )
        + FIXED_BINARY_SIZE;
  }

  /**
   * Deserializes a ColumnBinary from a byte array.
   * The data is created by the caller.
   *
   * @param metaBinary the byte array to deserialize.
   * @param start binary start position.
   * @param length binary length.
   * @param dataBuffer a byte array representing the data.
   *        Make a shallow copy to {@link #binary}.
   * @param childList the column list object of this column's children.
   *
   * @return A new deserialized ColumnBinary object.
   */
  public static ColumnBinary newInstanceFromMetaBinary(
      final byte[] metaBinary ,
      final int start ,
      final int length ,
      final byte[] dataBuffer ,
      final List<ColumnBinary> childList ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( metaBinary , start , length );

    int classNameLength = wrapBuffer.getInt();
    char[] classNameChars = new char[ classNameLength / Character.BYTES ];
    wrapBuffer.asCharBuffer().get( classNameChars );
    final String metaClassName = String.valueOf( classNameChars );
    wrapBuffer.position( wrapBuffer.position() + classNameLength );

    int compressorClassNameLength = wrapBuffer.getInt();
    char[] compressorClassNameChars = new char[ compressorClassNameLength / Character.BYTES ];
    wrapBuffer.asCharBuffer().get( compressorClassNameChars );
    final String metaCompressorClassName = String.valueOf( compressorClassNameChars );
    wrapBuffer.position( wrapBuffer.position() + compressorClassNameLength );

    int columnNameLength = wrapBuffer.getInt();
    char[] columnNameChars = new char[ columnNameLength / Character.BYTES ];
    wrapBuffer.asCharBuffer().get( columnNameChars );
    final String metaColumnName = String.valueOf( columnNameChars );
    wrapBuffer.position( wrapBuffer.position() + columnNameLength );

    byte columnTypeByte = wrapBuffer.get();
    ColumnType metaColumnType = ColumnTypeFactory.getColumnTypeFromByte( columnTypeByte );
    int metaRowCount = wrapBuffer.getInt();
    int metaRowData = wrapBuffer.getInt();
    int metaLogicalData = wrapBuffer.getInt();
    int metaCardinality = wrapBuffer.getInt();
    int metaBinaryStart = wrapBuffer.getInt();
    int metaBinaryLength = wrapBuffer.getInt();

    return new ColumnBinary(
        ColumnBinaryMakerNameShortCut.getClassName( metaClassName ) ,
        CompressorNameShortCut.getClassName( metaCompressorClassName ) ,
        metaColumnName ,
        metaColumnType ,
        metaRowCount ,
        metaRowData ,
        metaLogicalData ,
        metaCardinality ,
        dataBuffer ,
        metaBinaryStart ,
        metaBinaryLength ,
        childList );
  }

  /**
   * Serializes this object.
   * Does not include data.
   *
   * @return The new serialized byte array.
   */
  public byte[] toMetaBinary() {
    String shortCutClassName =
        ColumnBinaryMakerNameShortCut.getShortCutName( makerClassName );

    ByteBuffer wrapBuffer = ByteBuffer.allocate( getMetaSize() );

    int classNameLength = shortCutClassName.length() * 2;
    wrapBuffer.putInt( classNameLength );
    wrapBuffer.asCharBuffer().put( shortCutClassName.toCharArray() );
    wrapBuffer.position( wrapBuffer.position() + classNameLength );

    String shortCutCompressorClassName =
        CompressorNameShortCut.getShortCutName( compressorClassName );
    int compressorClassNameLength = shortCutCompressorClassName.length() * 2;
    wrapBuffer.putInt( compressorClassNameLength );
    wrapBuffer.asCharBuffer().put( shortCutCompressorClassName.toCharArray() );
    wrapBuffer.position( wrapBuffer.position() + compressorClassNameLength );

    int columnNameLength = columnName.length() * 2;
    wrapBuffer.putInt( columnNameLength );
    wrapBuffer.asCharBuffer().put( columnName.toCharArray() );
    wrapBuffer.position( wrapBuffer.position() + columnNameLength );

    byte columnTypeByte = ColumnTypeFactory.getColumnTypeByte( columnType );
    wrapBuffer.put( columnTypeByte );
    wrapBuffer.putInt( rowCount );
    wrapBuffer.putInt( rawDataSize );
    wrapBuffer.putInt( logicalDataSize );
    wrapBuffer.putInt( cardinality );
    wrapBuffer.putInt( binaryStart );
    wrapBuffer.putInt( binaryLength );

    return wrapBuffer.array();
  }

  /**
   * Convert this object to SummaryStats.
   *
   * @return Returns a new SummaryStats object containing the child elements.
   */
  public SummaryStats toSummaryStats() {
    SummaryStats stats = new SummaryStats(
        rowCount , rawDataSize , binaryLength , logicalDataSize , cardinality );
    if ( columnBinaryList != null ) {
      for ( ColumnBinary columnBinary : columnBinaryList ) {
        stats.merge( columnBinary.toSummaryStats() );
      }
    }
    return stats;
  }

  /**
   * Convert this object to ColumnStats.
   * In the case of Union, the elements of List are considered as one
   * column and are added together.
   * For other types, create an object containing the children.
   *
   * @return Returns a new ColumnStats object containing the child elements.
   */
  public ColumnStats toColumnStats() {
    ColumnStats columnStats = new ColumnStats( columnName );
    if ( columnType == ColumnType.UNION ) {
      for ( ColumnBinary columnBinary : columnBinaryList ) {
        columnStats.addSummaryStats( columnBinary.columnType , columnBinary.toSummaryStats() );
      }
    } else {
      SummaryStats stats = new SummaryStats(
          rowCount , rawDataSize , binaryLength , logicalDataSize , cardinality );
      columnStats.addSummaryStats( columnType , stats );
      for ( ColumnBinary columnBinary : columnBinaryList ) {
        columnStats.addChild( columnBinary.columnName , columnBinary.toColumnStats() );
      }
    }

    return columnStats;
  }

  /**
   * Sets the index of the element to be copied at load time.
   * This index is only used for loading, not when writing to a file.
   * The load index must be greater than 0 and equal to or greater than the previous number.
   */
  public void setLoadIndex( final int[] loadIndex ) {
    this.loadIndex = loadIndex;
  }

  /**
   * Create rename column binary.
   */
  public ColumnBinary createRenameColumnBinary( final String newName ) {
    ColumnBinary newColumnBinary = new ColumnBinary(
        makerClassName,
        compressorClassName,
        newName,
        columnType,
        rowCount,
        rawDataSize,
        logicalDataSize,
        cardinality,
        binary,
        binaryStart,
        binaryLength,
        columnBinaryList );
    newColumnBinary.loadIndex = loadIndex;
    return newColumnBinary;
  }

}
