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

public class DoubleRangeBlockIndex implements IBlockIndex {

  private Double min;
  private Double max;

  public DoubleRangeBlockIndex() {
    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
  }

  public DoubleRangeBlockIndex( final Double min , final Double max ) {
    this.min = min;
    this.max = max;
  }

  @Override
  public IBlockIndex clone() {
    return new DoubleRangeBlockIndex( min , max );
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.RANGE_DOUBLE;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( ! ( blockIndex instanceof DoubleRangeBlockIndex ) ) {
      return false;
    }
    DoubleRangeBlockIndex numberBlockIndex = (DoubleRangeBlockIndex)blockIndex;
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
    return Double.BYTES * 2;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putDouble( min );
    wrapBuffer.putDouble( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.getDouble();
    max = wrapBuffer.getDouble();
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    switch ( filter.getFilterType() ) {
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        Double setNumber;
        try {
          setNumber = Double.valueOf( numberFilter.getNumberObject().getDouble() );
        } catch ( NumberFormatException | IOException ex ) {
          return null;
        }
        switch ( numberFilter.getNumberFilterType() ) {
          case EQUAL:
            if ( 0 < min.compareTo( setNumber ) || max.compareTo( setNumber ) < 0 ) {
              return new ArrayList<Integer>();
            }
            return null;
          case LT:
            if ( 0 <= min.compareTo( setNumber ) ) {
              return new ArrayList<Integer>();
            }
            return null;
          case LE:
            if ( 0 < min.compareTo( setNumber ) ) {
              return new ArrayList<Integer>();
            }
            return null;
          case GT:
            if ( max.compareTo( setNumber ) <= 0 ) {
              return new ArrayList<Integer>();
            }
            return null;
          case GE:
            if ( max.compareTo( setNumber ) < 0 ) {
              return new ArrayList<Integer>();
            }
            return null;
          default:
            return null;
        }
      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        Double setMin;
        Double setMax;
        try {
          setMin = Double.valueOf( numberRangeFilter.getMinObject().getDouble() );
          setMax = Double.valueOf( numberRangeFilter.getMaxObject().getDouble() );
        } catch ( NumberFormatException | IOException ex ) {
          return null;
        }
        boolean minHasEquals = numberRangeFilter.isMinHasEquals();
        boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
        boolean invert = numberRangeFilter.isInvert();
        if ( invert ) {
          return null;
        }
        if ( minHasEquals && maxHasEquals ) {
          if ( ( 0 < min.compareTo( setMax ) || max.compareTo( setMin ) < 0 ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else if ( minHasEquals ) {
          if ( ( 0 <= min.compareTo( setMax ) || max.compareTo( setMin ) < 0 ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else if ( maxHasEquals ) {
          if ( ( 0 < min.compareTo( setMax ) || max.compareTo( setMin ) <= 0 ) ) {
            return new ArrayList<Integer>();
          }
          return null;
        } else {
          if ( ( 0 <= min.compareTo( setMax ) || max.compareTo( setMin ) <= 0 ) ) {
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
    return new DoubleRangeBlockIndex();
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

}
