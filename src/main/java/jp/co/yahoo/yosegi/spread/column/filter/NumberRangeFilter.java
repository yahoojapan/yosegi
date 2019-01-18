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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

public class NumberRangeFilter implements IFilter {

  private final boolean invert;
  private final PrimitiveObject minObj;
  private final boolean minHasEquals;
  private final PrimitiveObject maxObj;
  private final boolean maxHasEquals;

  public NumberRangeFilter(
      final PrimitiveObject minObj ,
      final boolean minHasEquals ,
      final PrimitiveObject maxObj ,
      final boolean maxHasEquals ) {
    this( false , minObj , minHasEquals , maxObj , maxHasEquals );
  }

  /**
   * Set comparison conditions and initialize.
   */
  public NumberRangeFilter(
      final boolean invert ,
      final PrimitiveObject minObj ,
      final boolean minHasEquals ,
      final PrimitiveObject maxObj ,
      final boolean maxHasEquals ) {
    this.invert = invert;
    this.minObj = minObj;
    this.minHasEquals = minHasEquals;
    this.maxObj = maxObj;
    this.maxHasEquals = maxHasEquals;
  }

  public boolean isInvert() {
    return invert;
  }

  public PrimitiveObject getMinObject() {
    return minObj;
  }

  public boolean isMinHasEquals() {
    return minHasEquals;
  }

  public PrimitiveObject getMaxObject() {
    return maxObj;
  }

  public boolean isMaxHasEquals() {
    return maxHasEquals;
  }

  @Override
  public FilterType getFilterType() {
    return FilterType.NUMBER_RANGE;
  }

}
