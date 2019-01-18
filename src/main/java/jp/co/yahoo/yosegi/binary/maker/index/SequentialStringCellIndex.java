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
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringComparator;
import jp.co.yahoo.yosegi.spread.column.filter.IStringCompareFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringDictionaryFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;

import java.io.IOException;
import java.util.Set;

public class SequentialStringCellIndex implements ICellIndex {

  private final IDicManager dicManager;

  public SequentialStringCellIndex( final IDicManager dicManager ) {
    this.dicManager = dicManager;
  }

  @Override
  public boolean[] filter(
      final IFilter filter , final boolean[] filterArray ) throws IOException {
    switch ( filter.getFilterType() ) {
      case STRING:
        IStringFilter stringFilter = (IStringFilter)filter;
        String targetStr = stringFilter.getSearchString();
        switch ( stringFilter.getStringFilterType() ) {
          case PERFECT:
            return perfectMatch( targetStr , filterArray );
          case PARTIAL:
            return partialMatch( targetStr , filterArray );
          case FORWARD:
            return forwardMatch( targetStr , filterArray );
          case BACKWARD:
            return backwardMatch( targetStr , filterArray );
          case REGEXP:
            return regexpMatch( targetStr , filterArray );
          default:
            return null;
        }
      case STRING_COMPARE:
        IStringCompareFilter stringCompareFilter = (IStringCompareFilter)filter;
        IStringComparator comparator = stringCompareFilter.getStringComparator();
        return compareString( comparator , filterArray );
      case STRING_DICTIONARY:
        IStringDictionaryFilter stringDictionaryFilter = (IStringDictionaryFilter)filter;
        Set<String> dictionary = stringDictionaryFilter.getDictionary();
        return dictionaryString( dictionary , filterArray );
      default:
        return null;
    }
  }

  private boolean[] dictionaryString(
      final Set<String> dictionary , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && dictionary.contains( obj.getString() ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

  private boolean[] compareString(
      final IStringComparator comparator , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj == null || ! comparator.isFilterString( obj.getString() ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

  private boolean[] perfectMatch(
      final String targetStr , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && targetStr.equals( obj.getString() ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

  private boolean[] partialMatch(
      final String targetStr , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && ( -1 < obj.getString().indexOf( targetStr) ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

  private boolean[] forwardMatch(
      final String targetStr , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && obj.getString().startsWith( targetStr ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

  private boolean[] backwardMatch(
      final String targetStr , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && obj.getString().endsWith( targetStr ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

  private boolean[] regexpMatch(
      final String targetStr , boolean[] filterArray ) throws IOException {
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && obj.getString().matches( targetStr ) ) {
        filterArray[i] = true;
      }
    }

    return filterArray;
  }

}
