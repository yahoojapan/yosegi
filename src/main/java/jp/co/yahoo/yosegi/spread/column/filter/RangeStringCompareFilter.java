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

package jp.co.yahoo.yosegi.spread.column.filter;

public class RangeStringCompareFilter implements IStringCompareFilter {

  private final boolean invert;
  private final IStringCompareFilter minFilter;
  private final IStringCompareFilter maxFilter;

  public RangeStringCompareFilter(
      final String min ,
      final boolean minHasEqual ,
      final String max ,
      final boolean maxHasEqual ) {
    this( min , minHasEqual , max , maxHasEqual , false );
  }

  /**
   * Set comparison conditions and initialize.
   */
  public RangeStringCompareFilter(
      final String min ,
      final boolean minHasEqual ,
      final String max ,
      final boolean maxHasEqual ,
      final boolean invert ) {
    this.invert = invert;
    if ( minHasEqual ) {
      minFilter = new GeStringCompareFilter( min );
    } else {
      minFilter = new GtStringCompareFilter( min );
    }
    if ( maxHasEqual ) {
      maxFilter = new LeStringCompareFilter( max );
    } else {
      maxFilter = new LtStringCompareFilter( max );
    }
  }

  @Override
  public IStringComparator getStringComparator() {
    return new RangeStringComparator(
        minFilter.getStringComparator() , maxFilter.getStringComparator() , invert );
  }

  @Override
  public StringCompareFilterType getStringCompareFilterType() {
    return StringCompareFilterType.RANGE;
  }

  @Override
  public FilterType getFilterType() {
    return FilterType.STRING_COMPARE;
  }

  private class RangeStringComparator implements IStringComparator {

    private final IStringComparator minComparator;
    private final IStringComparator maxComparator;
    private final boolean filter;
    private final boolean through;

    public RangeStringComparator(
        final IStringComparator minComparator ,
        final IStringComparator maxComparator ,
        final boolean invert ) {
      this.minComparator = minComparator;
      this.maxComparator = maxComparator;
      if ( invert ) {
        filter = false;
        through = true;
      } else {
        filter = true;
        through = false;
      }
    }

    @Override
    public boolean isFilterString( final String target ) {
      if ( minComparator.isFilterString( target ) || maxComparator.isFilterString( target ) ) {
        return filter;
      }
      return through;
    }

    @Override
    public boolean isOutOfRange( final String min , final String max ) {
      if ( invert ) {
        if ( ! minComparator.isFilterString( min ) && ! maxComparator.isFilterString( max ) ) {
          return true;
        }
        return false;
      }
      if ( minComparator.isFilterString( max ) || maxComparator.isFilterString( min ) ) {
        return filter;
      }
      return through;
    }

  }

}
