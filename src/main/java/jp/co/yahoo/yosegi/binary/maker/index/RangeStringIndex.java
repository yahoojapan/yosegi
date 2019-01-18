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

import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringComparator;
import jp.co.yahoo.yosegi.spread.column.filter.IStringCompareFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringDictionaryFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;

import java.io.IOException;
import java.util.Set;

public class RangeStringIndex implements ICellIndex {

  private final String max;
  private final String min;

  public RangeStringIndex( final String min , final String max ) {
    this.max = max;
    this.min = min;
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException {
    switch ( filter.getFilterType() ) {
      case STRING:
        IStringFilter stringFilter = (IStringFilter)filter;
        String targetStr = stringFilter.getSearchString();
        switch ( stringFilter.getStringFilterType() ) {
          case PERFECT:
            if ( min.compareTo( targetStr ) <= 0 && 0 <= max.compareTo( targetStr ) ) {
              return null;
            }
            return filterArray;
          case FORWARD:
            if ( min.startsWith( targetStr ) || ( 
                0 <= targetStr.compareTo( min ) && targetStr.compareTo( max ) <= 0 ) ) {
              return null;
            }
            return filterArray;
          default:
            return null;
        }
      case STRING_COMPARE:
        IStringCompareFilter stringCompareFilter = (IStringCompareFilter)filter;
        IStringComparator comparator = stringCompareFilter.getStringComparator();
        if ( comparator.isOutOfRange( min , max ) ) {
          return filterArray;
        }
        return null;
      case STRING_DICTIONARY:
        IStringDictionaryFilter stringDictionaryFilter = (IStringDictionaryFilter)filter;
        Set<String> dictionary = stringDictionaryFilter.getDictionary();
        for ( String str : dictionary ) {
          if ( min.compareTo( str ) <= 0 && 0 <= max.compareTo( str ) ) {
            return null;
          }
        }
        return filterArray;
      default:
        return null;
    }
  }

}
