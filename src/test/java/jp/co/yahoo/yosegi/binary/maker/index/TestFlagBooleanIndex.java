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
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TestFlagBooleanIndex {

  private static final boolean[] dummy = new boolean[0];

  public static Stream<Arguments> D_filter_1() {
    return Stream.of(
        arguments(new FlagBooleanIndex(true, false, false), new BooleanFilter(true), null),
        arguments(new FlagBooleanIndex(true, false, false), new BooleanFilter(false), dummy),
        arguments(new FlagBooleanIndex(true, true, false), new BooleanFilter(true), null),
        arguments(new FlagBooleanIndex(true, true, false), new BooleanFilter(false), null),
        arguments(new FlagBooleanIndex(true, false, true), new BooleanFilter(true), null),
        arguments(new FlagBooleanIndex(true, false, true), new BooleanFilter(false), dummy),
        arguments(new FlagBooleanIndex(true, true, true), new BooleanFilter(true), null),
        arguments(new FlagBooleanIndex(true, true, true), new BooleanFilter(false), null),
        arguments(new FlagBooleanIndex(false, true, false), new BooleanFilter(true), dummy),
        arguments(new FlagBooleanIndex(false, true, false), new BooleanFilter(false), null),
        arguments(new FlagBooleanIndex(false, false, true), new BooleanFilter(true), dummy),
        arguments(new FlagBooleanIndex(false, false, true), new BooleanFilter(false), dummy),
        arguments(new FlagBooleanIndex(false, true, true), new BooleanFilter(true), dummy),
        arguments(new FlagBooleanIndex(false, true, true), new BooleanFilter(false), null),
        arguments(new FlagBooleanIndex(false, false, false), new BooleanFilter(true), dummy),
        arguments(new FlagBooleanIndex(false, false, false), new BooleanFilter(false), dummy));
  }

  @ParameterizedTest
  @MethodSource("D_filter_1")
  public void T_filter_1(final ICellIndex cellIndex, final IFilter filter, final boolean[] result)
      throws IOException {
    boolean[] r = cellIndex.filter(filter, new boolean[0]);
    if (result == null) {
      assertNull(r);
    } else {
      assertEquals(result.length, r.length);
    }
  }
}
