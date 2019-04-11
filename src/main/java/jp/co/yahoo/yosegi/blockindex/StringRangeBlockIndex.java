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
import jp.co.yahoo.yosegi.spread.column.filter.IStringComparator;
import jp.co.yahoo.yosegi.spread.column.filter.IStringCompareFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringDictionaryFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IStringFilter;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StringRangeBlockIndex implements IBlockIndex {

  private String min;
  private String max;

  public StringRangeBlockIndex() {
    min = null;
    max = null;
  }

  public StringRangeBlockIndex( final String min , final String max ) {
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.RANGE_STRING;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    if ( !StringRangeBlockIndex.class.isInstance(blockIndex) ) {
      return false;
    }
    StringRangeBlockIndex stringRangeBlockIndex = (StringRangeBlockIndex)blockIndex;
    if ( 0 < min.compareTo( stringRangeBlockIndex.getMin() ) ) {
      min = stringRangeBlockIndex.getMin();
    }
    if ( max.compareTo( stringRangeBlockIndex.getMax() ) < 0 ) {
      max = stringRangeBlockIndex.getMax();
    }
    return true;
  }

  @Override
  public int getBinarySize() {
    return ( min.length() * 2 ) + ( max.length() * 2 ) + 4 + 4;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( min.length() );
    wrapBuffer.putInt( max.length() );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    viewCharBuffer.put( min.toCharArray() );
    viewCharBuffer.put( max.toCharArray() );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    wrapBuffer.position( start );
    int minSize = wrapBuffer.getInt();
    int maxSize = wrapBuffer.getInt();

    char[] minChars = new char[minSize];
    char[] maxChars = new char[maxSize];
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    viewCharBuffer.get( minChars );
    viewCharBuffer.get( maxChars );
    min = new String( minChars );
    max = new String( maxChars );
  }

  /**
   * From the filter condition, apply the filter corresponding to
   * this Index and obtain the index of Spread that needs to be read.
   */
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    switch ( filter.getFilterType() ) {
      case STRING:
        IStringFilter stringFilter = (IStringFilter)filter;
        String targetStr = stringFilter.getSearchString();
        switch ( stringFilter.getStringFilterType() ) {
          case PERFECT:
            if ( min.compareTo( targetStr ) <= 0
                && 0 <= max.compareTo( targetStr ) ) {
              return null;
            }
            return new ArrayList<Integer>();
          case FORWARD:
            if ( min.startsWith( targetStr )
                || ( 0 <= targetStr.compareTo( min ) && targetStr.compareTo( max ) <= 0 ) ) {
              return null;
            }
            return new ArrayList<Integer>();
          default:
            return null;
        }
      case STRING_COMPARE:
        IStringCompareFilter stringCompareFilter = (IStringCompareFilter)filter;
        IStringComparator comparator = stringCompareFilter.getStringComparator();
        if ( comparator.isOutOfRange( min , max ) ) {
          return new ArrayList<Integer>();
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
        return new ArrayList<Integer>();
      default:
        return null;
    }
  }

  @Override
  public IBlockIndex getNewInstance() {
    return new StringRangeBlockIndex();
  }

  public String getMin() {
    return min;
  }

  public String getMax() {
    return max;
  }

}
