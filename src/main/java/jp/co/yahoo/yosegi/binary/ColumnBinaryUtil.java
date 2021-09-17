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

package jp.co.yahoo.yosegi.binary;

import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.YosegiArrayIndexLoader;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;
import java.util.List;

public final class ColumnBinaryUtil {

  /**
   * Remove column binary from column name.
   */
  public static void removeFromColumnName(
        final String name , final List<ColumnBinary> binaryList ) {
    ColumnBinary result = null;
    if ( binaryList == null ) {
      return;
    }
    for ( int i = 0 ; i < binaryList.size() ; i++ ) {
      ColumnBinary child = binaryList.get(i);
      if ( name.equals( child.columnName ) ) {
        binaryList.remove(i);
        break;
      }
    }
    return;
  }

  /**
   * Get column binary from column name.
   */
  public static ColumnBinary getFromColumnName(
        final String name , final List<ColumnBinary> binaryList ) {
    ColumnBinary result = null;
    if ( binaryList == null ) {
      return result;
    }
    for ( int i = 0 ; i < binaryList.size() ; i++ ) {
      ColumnBinary child = binaryList.get(i);
      if ( name.equals( child.columnName ) ) {
        result = child;
        break;
      }
    }
    return result;
  }

  /**
   * Get column binary from column type.
   */
  public static ColumnBinary getFromColumnType(
        final ColumnType type , final List<ColumnBinary> binaryList ) {
    ColumnBinary result = null;
    if ( binaryList == null ) {
      return result;
    }
    for ( int i = 0 ; i < binaryList.size() ; i++ ) {
      ColumnBinary child = binaryList.get(i);
      if ( type.equals( child.columnType ) ) {
        result = child;
        break;
      }
    }
    return result;
  }

  /**
   * Get column from column binary.
   */
  public static IColumn createArrayIndexColumn( final ColumnBinary binary ) throws IOException {
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( binary.makerClassName );
    YosegiArrayIndexLoader indexColumnLoader =
        new YosegiArrayIndexLoader( binary , binary.rowCount );
    maker.load( binary , indexColumnLoader );
    return indexColumnLoader.build();
  }

  /**
   * Create array child index.
   */
  public static int[] createArrayChildIndex(
      final ColumnBinary arrayColumn , final int[] arrayLoadIndex ) throws IOException {
    if ( arrayLoadIndex.length == 0 ) {
      return new int[0];
    }
    IColumn arrayColumnTarget = createArrayIndexColumn( arrayColumn );
    int childLoadIndexLength = 0;
    int currentIndex = -1;
    for ( int i = 0; i < arrayLoadIndex.length ; i++ ) {
      if ( currentIndex == arrayLoadIndex[i] ) {
        continue;
      }
      currentIndex = arrayLoadIndex[i];
      ICell cell = arrayColumnTarget.get( arrayLoadIndex[i] );
      if ( cell.getType() != ColumnType.ARRAY ) {
        continue;
      }
      ArrayCell arrayCell = (ArrayCell)( cell );
      childLoadIndexLength += arrayCell.getEnd() - arrayCell.getStart();
    }
    int[] childLoadIndex = new int[childLoadIndexLength];
    int currentArrayIndex = 0;
    currentIndex = -1;
    for ( int i = 0; i < arrayLoadIndex.length ; i++ ) {
      if ( currentIndex == arrayLoadIndex[i] ) {
        continue;
      }
      currentIndex = arrayLoadIndex[i];
      ICell cell = arrayColumnTarget.get( arrayLoadIndex[i] );
      if ( cell.getType() != ColumnType.ARRAY ) {
        continue;
      }
      ArrayCell arrayCell = (ArrayCell)( cell );
      for ( int childIndex = arrayCell.getStart()
          ; childIndex < arrayCell.getEnd() ; childIndex++ ) {
        childLoadIndex[currentArrayIndex] = childIndex;
        currentArrayIndex++;
      }
    }
    return childLoadIndex;
  }

  /**
   * Set load index to column binary list.
   */
  public static void setLoadIndex(
      final List<ColumnBinary> binaryList , final int[] loadIndex ) throws IOException {
    if ( binaryList != null ) {
      for ( ColumnBinary child : binaryList ) {
        int[] childLoadIndex = loadIndex;
        if ( child.columnType == ColumnType.ARRAY ) {
          childLoadIndex = createArrayChildIndex( child , loadIndex );
        }
        setLoadIndex( child.columnBinaryList , childLoadIndex );
        child.setLoadIndex( loadIndex );
      }
    }
  }

}