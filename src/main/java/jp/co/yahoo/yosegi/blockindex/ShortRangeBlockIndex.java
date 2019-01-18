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
    if ( ! ( blockIndex instanceof ShortRangeBlockIndex ) ) {
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
            if ( setNumber < min || max < setNumber  ) {
              return new ArrayList<Integer>();
            }
            return null;
          case LT:
            if ( setNumber <= min ) {
              return new ArrayList<Integer>();
            }
            return null;
          case LE:
            if ( setNumber < min ) {
              return new ArrayList<Integer>();
            }
            return null;
          case GT:
            if ( max <= setNumber ) {
              return new ArrayList<Integer>();
            }
            return null;
          case GE:
            if ( max < setNumber ) {
              return new ArrayList<Integer>();
            }
            return null;
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
        boolean invert = numberRangeFilter.isInvert();
        if ( invert ) {
          return null;
        }
        boolean minHasEquals = numberRangeFilter.isMinHasEquals();
        boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
        if ( minHasEquals && maxHasEquals ) {
          if ( ( setMax < min || max < setMin ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else if ( minHasEquals ) {
          if ( ( setMax < min || max <= setMin ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else if ( maxHasEquals ) {
          if ( ( setMax <= min || max < setMin ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else {
          if ( ( setMax <= min || max <= setMin ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        }
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
