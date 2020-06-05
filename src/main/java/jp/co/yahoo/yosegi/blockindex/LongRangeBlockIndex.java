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

public class LongRangeBlockIndex implements IBlockIndex {

  private long min;
  private long max;

  public LongRangeBlockIndex() {
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
  }

  public LongRangeBlockIndex( final long min , final long max ) {
    this.min = min;
    this.max = max;
  }

  @Override
  public IBlockIndex clone() {
    return new LongRangeBlockIndex( min , max );
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.RANGE_LONG;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( ! ( blockIndex instanceof LongRangeBlockIndex ) ) {
      return false;
    }
    LongRangeBlockIndex numberBlockIndex = (LongRangeBlockIndex)blockIndex;
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
    return Long.BYTES * 2;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putLong( min );
    wrapBuffer.putLong( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.getLong();
    max = wrapBuffer.getLong();
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    switch ( filter.getFilterType() ) {
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        long setNumber;
        try {
          setNumber = numberFilter.getNumberObject().getLong();
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
        long setMin;
        long setMax;
        try {
          setMin = numberRangeFilter.getMinObject().getLong();
          setMax = numberRangeFilter.getMaxObject().getLong();
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
          if ( ( setMax <= min || max < setMin ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else if ( maxHasEquals ) {
          if ( ( setMax < min || max <= setMin ) ) {
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
    return new LongRangeBlockIndex();
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  @Override
  public String toString() {
    return String.format( "%s min=%d max=%d" , this.getClass().getName() , min , max );
  }

}
