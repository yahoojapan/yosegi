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

package jp.co.yahoo.yosegi.binary.maker.index;

import jp.co.yahoo.yosegi.binary.maker.IDicManager;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.filter.NumberFilter;
import jp.co.yahoo.yosegi.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.util.NumberUtils;

import java.io.IOException;

public class SequentialNumberCellIndex implements ICellIndex {

  private final IComparator comparator;
  private final IDicManager dicManager;

  /**
   * Index of Cell.
   */
  public SequentialNumberCellIndex(
      final ColumnType columnType , final IDicManager dicManager ) throws IOException {
    this.dicManager = dicManager;
    switch ( columnType ) {
      case BYTE:
        comparator = new ByteComparator();
        break;
      case SHORT:
        comparator = new ShortComparator();
        break;
      case INTEGER:
        comparator = new IntegerComparator();
        break;
      case LONG:
        comparator = new LongComparator();
        break;
      case FLOAT:
        comparator = new FloatComparator();
        break;
      case DOUBLE:
        comparator = new DoubleComparator();
        break;
      default:
        comparator = new NullComparator();
        break;
    }
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException {
    switch ( filter.getFilterType() ) {
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        switch ( numberFilter.getNumberFilterType() ) {
          case EQUAL:
            return comparator.getEqual( filterArray , dicManager , numberFilter );
          case NOT_EQUAL:
            return comparator.getNotEqual( filterArray , dicManager , numberFilter );
          case LT:
            return comparator.getLt( filterArray , dicManager , numberFilter );
          case LE:
            return comparator.getLe( filterArray , dicManager , numberFilter );
          case GT:
            return comparator.getGt( filterArray , dicManager , numberFilter );
          case GE:
            return comparator.getGe( filterArray , dicManager , numberFilter );
          default:
            return null;
        }
      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        return comparator.getRange( filterArray , dicManager , numberRangeFilter );
      default:
        return null;
    }
  }

  public interface IComparator {

    boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException;

    boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException;

    boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException;

    boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException;

    boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException;

    boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException;

    boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException;

  }

  public class NullComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      return null;
    }

  }

  public class LongComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      long target;
      try {
        target = numberFilter.getNumberObject().getLong();
      } catch ( NumberFormatException ex ) {
        return filterArray;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target == numObj.getLong() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      long target;
      try {
        target = numberFilter.getNumberObject().getLong();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null || target != numObj.getLong() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      long target;
      try {
        target = numberFilter.getNumberObject().getLong();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getLong() < target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      long target;
      try {
        target = numberFilter.getNumberObject().getLong();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getLong() <= target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      long target;
      try {
        target = numberFilter.getNumberObject().getLong();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target < numObj.getLong() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      long target;
      try {
        target = numberFilter.getNumberObject().getLong();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target <= numObj.getLong() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      boolean invert = numberRangeFilter.isInvert();
      if ( invert ) {
        return null;
      }
      long min;
      long max;
      try {
        min = numberRangeFilter.getMinObject().getLong();
        max = numberRangeFilter.getMaxObject().getLong();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        long target = numObj.getLong();
        if ( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) ) {
          filterArray[i] = true;
        }
      }
      return filterArray;
    }

  }

  public class IntegerComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      int target;
      try {
        target = numberFilter.getNumberObject().getInt();
      } catch ( NumberFormatException ex ) {
        return filterArray;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target == numObj.getInt() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      int target;
      try {
        target = numberFilter.getNumberObject().getInt();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null || target != numObj.getInt() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      int target;
      try {
        target = numberFilter.getNumberObject().getInt();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getInt() < target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      int target;
      try {
        target = numberFilter.getNumberObject().getInt();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getInt() <= target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      int target;
      try {
        target = numberFilter.getNumberObject().getInt();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target < numObj.getInt() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      int target;
      try {
        target = numberFilter.getNumberObject().getInt();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target <= numObj.getInt() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      boolean invert = numberRangeFilter.isInvert();
      if ( invert ) {
        return null;
      }
      int min;
      int max;
      try {
        min = numberRangeFilter.getMinObject().getInt();
        max = numberRangeFilter.getMaxObject().getInt();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        int target = numObj.getInt();
        if ( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) ) {
          filterArray[i] = true;
        }
      }
      return filterArray;
    }

  }

  public class ShortComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      short target;
      try {
        target = numberFilter.getNumberObject().getShort();
      } catch ( NumberFormatException ex ) {
        return filterArray;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target == numObj.getShort() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      short target;
      try {
        target = numberFilter.getNumberObject().getShort();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null || target != numObj.getShort() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      short target;
      try {
        target = numberFilter.getNumberObject().getShort();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getShort() < target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      short target;
      try {
        target = numberFilter.getNumberObject().getShort();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getShort() <= target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      short target;
      try {
        target = numberFilter.getNumberObject().getShort();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target < numObj.getShort() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      short target;
      try {
        target = numberFilter.getNumberObject().getShort();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target <= numObj.getShort() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      boolean invert = numberRangeFilter.isInvert();
      if ( invert ) {
        return null;
      }
      short min;
      short max;
      try {
        min = numberRangeFilter.getMinObject().getShort();
        max = numberRangeFilter.getMaxObject().getShort();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        short target = numObj.getShort();
        if ( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) ) {
          filterArray[i] = true;
        }
      }
      return filterArray;
    }

  }

  public class ByteComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      byte target;
      try {
        target = numberFilter.getNumberObject().getByte();
      } catch ( NumberFormatException ex ) {
        return filterArray;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target == numObj.getByte() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      byte target;
      try {
        target = numberFilter.getNumberObject().getByte();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null || target != numObj.getByte() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      byte target;
      try {
        target = numberFilter.getNumberObject().getByte();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getByte() < target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      byte target;
      try {
        target = numberFilter.getNumberObject().getByte();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( numObj.getByte() <= target ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      byte target;
      try {
        target = numberFilter.getNumberObject().getByte();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target < numObj.getByte() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      byte target;
      try {
        target = numberFilter.getNumberObject().getByte();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target <= numObj.getByte() ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      boolean invert = numberRangeFilter.isInvert();
      if ( invert ) {
        return null;
      }
      byte min;
      byte max;
      try {
        min = numberRangeFilter.getMinObject().getByte();
        max = numberRangeFilter.getMaxObject().getByte();
      } catch ( NumberFormatException ex ) {
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        byte target = numObj.getByte();
        if ( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) ) {
          filterArray[i] = true;
        }
      }
      return filterArray;
    }

  }

  public class FloatComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Float target;
      try {
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( 0 < target.compareTo( numObj.getFloat() ) ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Float target;
      try {
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( 0 <= target.compareTo( numObj.getFloat() ) ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Float target;
      try {
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target.compareTo( numObj.getFloat() ) < 0 ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Float target;
      try {
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target.compareTo( numObj.getFloat() ) <= 0 ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      boolean invert = numberRangeFilter.isInvert();
      if ( invert ) {
        return null;
      }
      Float min;
      Float max;
      try {
        min = Float.valueOf( numberRangeFilter.getMinObject().getFloat() );
        max = Float.valueOf( numberRangeFilter.getMaxObject().getFloat() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        Float target = Float.valueOf( numObj.getFloat() );
        if ( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) ) {
          filterArray[i] = true;
        }
      }
      return filterArray;
    }

  }

  public class DoubleComparator implements IComparator {

    @Override
    public boolean[] getEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getNotEqual(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      return null;
    }

    @Override
    public boolean[] getLt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Double target;
      try {
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( 0 < target.compareTo( numObj.getDouble() ) ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getLe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Double target;
      try {
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( 0 <= target.compareTo( numObj.getDouble() ) ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGt(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Double target;
      try {
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target.compareTo( numObj.getDouble() ) < 0 ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getGe(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberFilter numberFilter ) throws IOException {
      Double target;
      try {
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        if ( target.compareTo( numObj.getDouble() ) <= 0 ) {
          filterArray[i] = true;
        }
      }

      return filterArray;
    }

    @Override
    public boolean[] getRange(
        final boolean[] filterArray ,
        final IDicManager dicManager ,
        final NumberRangeFilter numberRangeFilter ) throws IOException {
      boolean invert = numberRangeFilter.isInvert();
      if ( invert ) {
        return null;
      }
      Double min;
      Double max;
      try {
        min = Double.valueOf( numberRangeFilter.getMinObject().getDouble() );
        max = Double.valueOf( numberRangeFilter.getMaxObject().getDouble() );
      } catch ( NumberFormatException ex ) {
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
        PrimitiveObject numObj = dicManager.get( i );
        if ( numObj == null ) {
          continue;
        }
        Double target = Double.valueOf( numObj.getDouble() );
        if ( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) ) {
          filterArray[i] = true;
        }
      }
      return filterArray;
    }

  }

}
