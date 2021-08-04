/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.binary.maker.index;

import jp.co.yahoo.yosegi.spread.column.filter.BooleanFilter;
import jp.co.yahoo.yosegi.spread.column.filter.FilterType;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;

import java.io.IOException;

public class FlagBooleanIndex implements ICellIndex {

  private final boolean hasTrue;
  private final boolean hasFalse;
  private final boolean hasNull;

  /**
   * FlagBooleanIndex constructor.
   * @param hasTrue true when true values exist.
   * @param hasFalse true when false values exist.
   * @param hasNull true when null values exist.
   */
  public FlagBooleanIndex(final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
    this.hasTrue = hasTrue;
    this.hasFalse = hasFalse;
    this.hasNull = hasNull;
  }

  @Override
  public boolean[] filter(final IFilter filter, final boolean[] filterArray) throws IOException {
    if (filter.getFilterType() == FilterType.BOOLEAN) {
      BooleanFilter booleanFilter = (BooleanFilter) filter;
      // NOTE: return filterArray filled with false when there is no target value
      if (booleanFilter.getFlag()) {
        if (!hasTrue) {
          return filterArray;
        }
      } else {
        if (!hasFalse) {
          return filterArray;
        }
      }
    }
    // NOTE: return null when there are some target values
    return null;
  }
}
