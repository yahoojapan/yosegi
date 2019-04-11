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

package jp.co.yahoo.yosegi.blockindex;

import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FullRangeBlockIndex implements IBlockIndex {

  public List<RangeBlockIndex> childList = new ArrayList<RangeBlockIndex>();

  public FullRangeBlockIndex() {}

  /**
   * Set an initial value to create a new object.
   */
  public FullRangeBlockIndex( final int spreadIndex , final IBlockIndex index ) {
    switch ( index.getBlockIndexType() ) {
      case RANGE_STRING:
      case RANGE_BYTE:
      case RANGE_SHORT:
      case RANGE_INTEGER:
      case RANGE_LONG:
      case RANGE_FLOAT:
      case RANGE_DOUBLE:
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupport index type : " + index.getBlockIndexType() );
    }
    childList.add( new RangeBlockIndex( spreadIndex , index ) );
  }

  private final class RangeBlockIndex {

    private final int index;
    private final IBlockIndex blockIndex;

    public RangeBlockIndex( final int index , final IBlockIndex blockIndex ) {
      this.index = index;
      this.blockIndex = blockIndex;
    }

    public int getIndex() {
      return index;
    }

    public IBlockIndex getBlockIndex() {
      return blockIndex;
    }

  }

  /**
   * Determine the type of block index and convert it to bytes.
   */
  public byte getTypeToByte( final BlockIndexType type ) {
    switch ( type ) {
      case RANGE_STRING:
        return 0;
      case RANGE_BYTE:
        return 1;
      case RANGE_SHORT:
        return 2;
      case RANGE_INTEGER:
        return 3;
      case RANGE_LONG:
        return 4;
      case RANGE_FLOAT:
        return 5;
      case RANGE_DOUBLE:
        return 6;
      default:
        throw new UnsupportedOperationException( "Unsupport index type : " + type );
    }
  }

  /**
   * Determine byte and obtain IBlockIndex.
   */
  public IBlockIndex getByteToBlockIndex( final byte type ) {
    switch ( type ) {
      case 0:
        return new StringRangeBlockIndex();
      case 1:
        return new ByteRangeBlockIndex();
      case 2:
        return new ShortRangeBlockIndex();
      case 3:
        return new IntegerRangeBlockIndex();
      case 4:
        return new LongRangeBlockIndex();
      case 5:
        return new FloatRangeBlockIndex();
      case 6:
        return new DoubleRangeBlockIndex();
      default:
        throw new UnsupportedOperationException( "Unsupport index type"  );
    }
  }

  public List<RangeBlockIndex> getBlockIndexList() {
    return childList;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.FULL_RANGE;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( !FullRangeBlockIndex.class.isInstance(blockIndex) ) {
      return false;
    }
    FullRangeBlockIndex fullRangeBlockIndex = (FullRangeBlockIndex)blockIndex;
    childList.addAll( fullRangeBlockIndex.getBlockIndexList() );

    return true;
  }

  @Override
  public int getBinarySize() {
    int total = 0;
    for ( RangeBlockIndex index : childList ) {
      total += index.getBlockIndex().getBinarySize();
    }
    return Integer.BYTES + ( ( Byte.BYTES + Integer.BYTES * 2 ) * childList.size() ) + total;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( childList.size() );
    for ( RangeBlockIndex index : childList ) {
      byte[] childBinary = index.getBlockIndex().toBinary();
      wrapBuffer.put( getTypeToByte( index.getBlockIndex().getBlockIndexType() ) );
      wrapBuffer.putInt( index.getIndex() );
      wrapBuffer.putInt( childBinary.length );
      wrapBuffer.put( childBinary );
    }
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
    int num = wrapBuffer.getInt();

    for ( int i = 0 ; i < num ; i++ ) {
      byte type = wrapBuffer.get();
      int spreadIndex = wrapBuffer.getInt();
      int binaryLength = wrapBuffer.getInt();
      byte[] childBinary = new byte[binaryLength];
      wrapBuffer.get( childBinary , 0 , childBinary.length );
      IBlockIndex blockIndex = getByteToBlockIndex( type );
      blockIndex.setFromBinary( childBinary , 0 , childBinary.length );
      childList.add( new RangeBlockIndex( spreadIndex , blockIndex ) );
    }
  }

  /**
   * Get the index of Spread that needs to be read.
   */
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    List<Integer> result = new ArrayList<Integer>();
    for ( RangeBlockIndex index : childList ) {
      List<Integer> childResult = index.getBlockIndex().getBlockSpreadIndex( filter );
      if ( childResult == null ) {
        result.add( index.getIndex() );
      }
    }
    return result;
  }

  @Override
  public IBlockIndex getNewInstance() {
    return new FullRangeBlockIndex();
  }

}
