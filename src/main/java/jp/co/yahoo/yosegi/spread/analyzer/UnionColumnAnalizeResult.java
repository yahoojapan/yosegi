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

import java.util.List;

public class UnionColumnAnalizeResult implements IColumnAnalizeResult {

  private final String columnName;
  private final int columnSize;
  private final int nullCount;
  private final List<IColumnAnalizeResult> childResult;

  /**
   * Set and initialize results.
   */
  public UnionColumnAnalizeResult(
      final String columnName ,
      final int columnSize ,
      final int nullCount ,
      final List<IColumnAnalizeResult> childResult ) {
    this.columnName = columnName;
    this.columnSize = columnSize;
    this.nullCount = nullCount;
    this.childResult = childResult;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public ColumnType getColumnType() {
    return ColumnType.UNION;
  }

  @Override
  public int getColumnSize() {
    return columnSize;
  }

  @Override
  public boolean maybeSorted() {
    return false;
  }

  @Override
  public int getNullCount() {
    return nullCount;
  }

  @Override
  public int getRowCount() {
    return columnSize;
  }

  @Override
  public int getUniqCount() {
    return columnSize;
  }

  @Override
  public int getLogicalDataSize() {
    return 0;
  }

  @Override
  public int getRowStart() {
    return 0;
  }

  @Override
  public int getRowEnd() {
    return columnSize - 1;
  }

  @Override
  public List<IColumnAnalizeResult> getChild() {
    return childResult;
  }

}
