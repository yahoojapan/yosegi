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
import java.nio.IntBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class BufferDirectSequentialStringCellIndex implements ICellIndex {

  private final IDicManager dicManager;
  private final IntBuffer dicIndexIntBuffer;

  public BufferDirectSequentialStringCellIndex(
      final IDicManager dicManager , final IntBuffer dicIndexIntBuffer ) {
    this.dicManager = dicManager;
    this.dicIndexIntBuffer = dicIndexIntBuffer;
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
            return toColumnList( perfectMatch( targetStr ) , filterArray );
          case PARTIAL:
            return toColumnList( partialMatch( targetStr ) , filterArray );
          case FORWARD:
            return toColumnList( forwardMatch( targetStr ) , filterArray );
          case BACKWARD:
            return toColumnList( backwardMatch( targetStr ) , filterArray );
          case REGEXP:
            return toColumnList( regexpMatch( targetStr ) , filterArray );
          default:
            return null;
        }
      case STRING_COMPARE:
        IStringCompareFilter stringCompareFilter = (IStringCompareFilter)filter;
        IStringComparator comparator = stringCompareFilter.getStringComparator();
        return toColumnList( compareString( comparator ) , filterArray );
      case STRING_DICTIONARY:
        IStringDictionaryFilter stringDictionaryFilter = (IStringDictionaryFilter)filter;
        Set<String> dictionary = stringDictionaryFilter.getDictionary();
        return toColumnList( dictionaryString( dictionary ) , filterArray );
      default:
        return null;
    }
  }

  private boolean[] toColumnList(
      final Set<Integer> targetDicSet , final boolean[] filterArray ) {
    if ( targetDicSet.isEmpty() ) {
      return filterArray;
    }
    int length = dicIndexIntBuffer.capacity();
    for ( int i = 0 ; i < length ; i++ ) {
      Integer dicIndex = Integer.valueOf( dicIndexIntBuffer.get(i) );
      if ( targetDicSet.contains( dicIndex ) ) {
        filterArray[i] = true;
      }
    }
    return filterArray;
  }

  private Set<Integer> dictionaryString( final Set<String> dictionary ) throws IOException {
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && dictionary.contains( obj.getString() ) ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> compareString( final IStringComparator comparator ) throws IOException {
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj == null || ! comparator.isFilterString( obj.getString() ) ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> perfectMatch( final String targetStr ) throws IOException {
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && targetStr.equals( obj.getString() ) ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> partialMatch( final String targetStr ) throws IOException {
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && ( -1 < obj.getString().indexOf( targetStr) ) ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> forwardMatch( final String targetStr ) throws IOException {
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && obj.getString().startsWith( targetStr ) ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> backwardMatch( final String targetStr ) throws IOException {
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && obj.getString().endsWith( targetStr ) ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> regexpMatch( final String targetStr ) throws IOException {
    Pattern pt = Pattern.compile( targetStr );
    Set<Integer> matchDicList = new HashSet<Integer>();
    for ( int i = 0 ; i < dicManager.getDicSize() ; i++ ) {
      PrimitiveObject obj = dicManager.get( i );
      if ( obj != null && pt.matcher( obj.getString() ).find() ) {
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

}
