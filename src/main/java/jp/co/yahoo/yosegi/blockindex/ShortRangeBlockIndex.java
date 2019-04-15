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
import jp.co.yahoo.yosegi.spread.column.filter.NumberFilter;
import jp.co.yahoo.yosegi.spread.column.filter.NumberRangeFilter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

public class ShortRangeBlockIndex implements IBlockIndex {

  private short min;
  private short max;

  public ShortRangeBlockIndex() {
    min = Short.MAX_VALUE;
    max = Short.MIN_VALUE;
  }

  public ShortRangeBlockIndex( final short min , final short max ) {
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.RANGE_SHORT;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( !ShortRangeBlockIndex.class.isInstance(blockIndex) ) {
      return false;
    }
    ShortRangeBlockIndex numberBlockIndex = (ShortRangeBlockIndex)blockIndex;
    if ( numberBlockIndex.getMin() < min ) {
      min = numberBlockIndex.getMin();
    }
    if ( max < numberBlockIndex.getMax() ) {
      max = numberBlockIndex.getMax();
    }
    return true;
  }

  @Override
  public int getBinarySize() {
    return Short.BYTES * 2;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putShort( min );
    wrapBuffer.putShort( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.getShort();
    max = wrapBuffer.getShort();
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    switch ( filter.getFilterType() ) {
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        short setNumber;
        try {
          setNumber = numberFilter.getNumberObject().getShort();
        } catch ( NumberFormatException | IOException ex ) {
          return null;
        }
        switch ( numberFilter.getNumberFilterType() ) {
          case EQUAL:
            return (setNumber < min || max < setNumber) ? new ArrayList<Integer>() : null;
          case LT:
            return (setNumber <= min) ? new ArrayList<Integer>() : null;
          case LE:
            return (setNumber < min) ? new ArrayList<Integer>() : null;
          case GT:
            return (max <= setNumber) ? new ArrayList<Integer>() : null;
          case GE:
            return (max < setNumber) ? new ArrayList<Integer>() : null;
          default:
            return null;
        }

      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        short setMin;
        short setMax;
        try {
          setMin = numberRangeFilter.getMinObject().getShort();
          setMax = numberRangeFilter.getMaxObject().getShort();
        } catch ( NumberFormatException | IOException ex ) {
          return null;
        }
        if (numberRangeFilter.isInvert()) {
          return null;
        }
        boolean minHasEquals = numberRangeFilter.isMinHasEquals();
        boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
        BooleanSupplier isMin = () -> minHasEquals ? min <= setMax : min < setMax;
        BooleanSupplier isMax = () -> maxHasEquals ? setMin <= max : setMin < max;
        return (isMin.getAsBoolean() && isMax.getAsBoolean()) ? null : new ArrayList<Integer>();

      default:
        return null;
    }
  }

  @Override
  public IBlockIndex getNewInstance() {
    return new ShortRangeBlockIndex();
  }

  public short getMin() {
    return min;
  }

  public short getMax() {
    return max;
  }

}
