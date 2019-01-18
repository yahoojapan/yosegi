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

package jp.co.yahoo.yosegi.spread.analyzer;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class BytesColumnAnalizeResult implements IColumnAnalizeResult {

  private final String columnName;
  private final int columnSize;
  private final boolean sortFlag;
  private final int nullCount;
  private final int rowCount;
  private final int uniqCount;
  private final int totalLogicalDataSize;
  private final int minBytes;
  private final int maxBytes;

  /**
   * Set and initialize results.
   */
  public BytesColumnAnalizeResult(
      final String columnName ,
      final int columnSize ,
      final boolean sortFlag ,
      final int nullCount ,
      final int rowCount ,
      final int uniqCount ,
      final int totalLogicalDataSize ,
      final int minBytes ,
      final int maxBytes ) {
    this.columnName = columnName;
    this.columnSize = columnSize;
    this.sortFlag = sortFlag;
    this.nullCount = nullCount;
    this.rowCount = rowCount;
    this.uniqCount = uniqCount;
    this.totalLogicalDataSize = totalLogicalDataSize;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public ColumnType getColumnType() {
    return ColumnType.BYTES;
  }

  @Override
  public int getColumnSize() {
    return columnSize;
  }

  @Override
  public boolean maybeSorted() {
    return sortFlag;
  }

  @Override
  public int getNullCount() {
    return nullCount;
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  @Override
  public int getUniqCount() {
    return uniqCount;
  }

  @Override
  public int getLogicalDataSize() {
    return totalLogicalDataSize;
  }

  @Override
  public int getRowStart() {
    return 0;
  }

  @Override
  public int getRowEnd() {
    return columnSize - 1;
  }

  public int getMinBytes() {
    return minBytes;
  }

  public int getMaxBytes() {
    return maxBytes;
  }

}
