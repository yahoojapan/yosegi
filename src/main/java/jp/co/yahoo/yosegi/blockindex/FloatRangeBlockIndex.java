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

public class FloatRangeBlockIndex implements IBlockIndex {
  @FunctionalInterface
  public interface DispatchedFunc {
    public boolean apply(Float setNumber, Float min, Float max);
  }

  private static EnumDispatcherFactory.Func<NumberFilterType, DispatchedFunc>
      numberFilterTypeDispatcher;

  static {
    EnumDispatcherFactory<NumberFilterType, DispatchedFunc> sw =
        new EnumDispatcherFactory<>(NumberFilterType.class);
    sw.setDefault((setNumber, min, max) -> false);
    sw.set(NumberFilterType.EQUAL, (setNumber, min, max) ->
        ((0 < min.compareTo(setNumber) || max.compareTo(setNumber) < 0)));
    sw.set(NumberFilterType.LT, (setNumber, min, max) -> (0 <= min.compareTo(setNumber)));
    sw.set(NumberFilterType.LE, (setNumber, min, max) -> (0 <  min.compareTo(setNumber)));
    sw.set(NumberFilterType.GT, (setNumber, min, max) -> (max.compareTo(setNumber) <= 0));
    sw.set(NumberFilterType.GE, (setNumber, min, max) -> (max.compareTo(setNumber) <  0));
    numberFilterTypeDispatcher = sw.create();
  }

  private Float min;
  private Float max;

  public FloatRangeBlockIndex() {
    min = Float.MAX_VALUE;
    max = Float.MIN_VALUE;
  }

  public FloatRangeBlockIndex( final Float min , final Float max ) {
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.RANGE_FLOAT;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( !FloatRangeBlockIndex.class.isInstance(blockIndex) ) {
      return false;
    }
    FloatRangeBlockIndex numberBlockIndex = (FloatRangeBlockIndex)blockIndex;
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
    return Float.BYTES * 2;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putFloat( min );
    wrapBuffer.putFloat( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.getFloat();
    max = wrapBuffer.getFloat();
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    switch ( filter.getFilterType() ) {
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        Float setNumber;
        try {
          setNumber = Float.valueOf( numberFilter.getNumberObject().getFloat() );
        } catch ( NumberFormatException | IOException ex ) {
          return null;
        }
        return numberFilterTypeDispatcher
            .get(numberFilter.getNumberFilterType())
            .apply(setNumber, min, max) ? new ArrayList<Integer>() : null;

      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        Float setMin;
        Float setMax;
        try {
          setMin = Float.valueOf( numberRangeFilter.getMinObject().getFloat() );
          setMax = Float.valueOf( numberRangeFilter.getMaxObject().getFloat() );
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
    return new FloatRangeBlockIndex();
  }

  public Float getMin() {
    return min;
  }

  public Float getMax() {
    return max;
  }

}
