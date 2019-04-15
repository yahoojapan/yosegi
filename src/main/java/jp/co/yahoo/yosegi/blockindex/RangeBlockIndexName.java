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

import jp.co.yahoo.yosegi.util.INamePair;

public enum RangeBlockIndexName implements INamePair {
  R0("R0", "jp.co.yahoo.yosegi.blockindex.ByteRangeBlockIndex"),
  R1("R1", "jp.co.yahoo.yosegi.blockindex.ShortRangeBlockIndex"),
  R2("R2", "jp.co.yahoo.yosegi.blockindex.IntegerRangeBlockIndex"),
  R3("R3", "jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex"),
  R4("R4", "jp.co.yahoo.yosegi.blockindex.FloatRangeBlockIndex"),
  R5("R5", "jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex"),
  R6("R6", "jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex"),
  FR0("FR0", "jp.co.yahoo.yosegi.blockindex.FullRangeBlockIndex"),
  ;

  private final String shortName;
  private final String longName;

  private RangeBlockIndexName(final String shortName, final String longName) {
    this.shortName = shortName;
    this.longName = longName;
  }

  public String getLongName() {
    return longName;
  }

  public String getShortName() {
    return shortName;
  }
}

