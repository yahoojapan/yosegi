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
import jp.co.yahoo.yosegi.spread.column.filter.NumberFilterType;
import jp.co.yahoo.yosegi.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.yosegi.util.EnumDispatcherFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

public class IntegerRangeBlockIndex implements IBlockIndex {
  @FunctionalInterface
  public interface DispatchedFunc {
    public boolean apply(int setNumber, int min, int max);
  }

  private static EnumDispatcherFactory.Func<NumberFilterType, DispatchedFunc>
      numberFilterTypeDispatcher;

  static {
    EnumDispatcherFactory<NumberFilterType, DispatchedFunc> sw =
        new EnumDispatcherFactory<>(NumberFilterType.class);
    sw.setDefault((setNumber, min, max) -> false);
    sw.set(NumberFilterType.EQUAL, (setNumber, min, max) -> (setNumber < min || max < setNumber));
    sw.set(NumberFilterType.LT, (setNumber, min, max) -> (setNumber <= min));
    sw.set(NumberFilterType.LE, (setNumber, min, max) -> (setNumber <  min));
    sw.set(NumberFilterType.GT, (setNumber, min, max) -> (max <= setNumber));
    sw.set(NumberFilterType.GE, (setNumber, min, max) -> (max <  setNumber));
    numberFilterTypeDispatcher = sw.create();
  }

  private int min;
  private int max;

  public IntegerRangeBlockIndex() {
    min = Integer.MAX_VALUE;
    max = Integer.MIN_VALUE;
  }

  public IntegerRangeBlockIndex( final int min , final int max ) {
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.RANGE_INTEGER;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( !IntegerRangeBlockIndex.class.isInstance(blockIndex) ) {
      return false;
    }
    IntegerRangeBlockIndex numberBlockIndex = (IntegerRangeBlockIndex)blockIndex;
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
    return Integer.BYTES * 2;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( min );
    wrapBuffer.putInt( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.getInt();
    max = wrapBuffer.getInt();
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    switch ( filter.getFilterType() ) {
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        int setNumber;
        try {
          setNumber = numberFilter.getNumberObject().getInt();
        } catch ( NumberFormatException | IOException ex ) {
          return null;
        }
        return numberFilterTypeDispatcher
            .get(numberFilter.getNumberFilterType())
            .apply(setNumber, min, max) ? new ArrayList<Integer>() : null;

      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        int setMin;
        int setMax;
        try {
          setMin = numberRangeFilter.getMinObject().getInt();
          setMax = numberRangeFilter.getMaxObject().getInt();
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
    return new IntegerRangeBlockIndex();
  }

  public int getMin() {
    return min;
  }

  public int getMax() {
    return max;
  }

}
