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

package jp.co.yahoo.yosegi.blockindex;

import jp.co.yahoo.yosegi.spread.column.filter.BooleanFilter;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class TestBooleanBlockIndex {

  @Test
  public void T_newInstance_1() {
    BooleanBlockIndex blockIndex = new BooleanBlockIndex();
    assertFalse(blockIndex.hasTrue());
    assertFalse(blockIndex.hasFalse());
    assertFalse(blockIndex.hasNull());
  }

  public static Stream<Arguments> D_newInstance_2() {
    // hasTrue, hasFalse, hasNull
    return Stream.of(
        arguments(true, true, true),
        arguments(true, true, false),
        arguments(true, false, true),
        arguments(true, false, false),
        arguments(false, false, false),
        arguments(false, false, true),
        arguments(false, true, false),
        arguments(false, true, true));
  }

  @ParameterizedTest
  @MethodSource("D_newInstance_2")
  public void T_newInstance_2(
      final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
    BooleanBlockIndex blockIndex = new BooleanBlockIndex(hasTrue, hasFalse, hasNull);
    assertEquals(hasTrue, blockIndex.hasTrue());
    assertEquals(hasFalse, blockIndex.hasFalse());
    assertEquals(hasNull, blockIndex.hasNull());
  }

  @Test
  public void T_getBlockIndexType_1() {
    BooleanBlockIndex blockIndex = new BooleanBlockIndex();
    assertEquals(BlockIndexType.BOOLEAN, blockIndex.getBlockIndexType());
  }

  public static Stream<Arguments> D_merge_1() {
    return Stream.of(
        // initial values, merge values, expected values
        arguments(
            new Boolean[] {false, false, false},
            new Boolean[] {true, false, false},
            new Boolean[] {true, false, false}),
        arguments(
            new Boolean[] {false, false, false},
            new Boolean[] {false, true, false},
            new Boolean[] {false, true, false}),
        arguments(
            new Boolean[] {false, false, false},
            new Boolean[] {false, false, true},
            new Boolean[] {false, false, true}),
        arguments(
            new Boolean[] {true, false, false},
            new Boolean[] {false, false, false},
            new Boolean[] {true, false, false}),
        arguments(
            new Boolean[] {false, true, false},
            new Boolean[] {false, false, false},
            new Boolean[] {false, true, false}),
        arguments(
            new Boolean[] {true, true, true},
            new Boolean[] {true, true, true},
            new Boolean[] {true, true, true}),
        arguments(
            new Boolean[] {false, false, false},
            new Boolean[] {false, false, false},
            new Boolean[] {false, false, false}),
        arguments(
            new Boolean[] {false, false, false},
            new Boolean[] {true, true, true},
            new Boolean[] {true, true, true}),
        arguments(
            new Boolean[] {true, true, true},
            new Boolean[] {false, false, false},
            new Boolean[] {true, true, true}));
  }

  @ParameterizedTest
  @MethodSource("D_merge_1")
  public void T_merge_1(final Boolean[] initial, final Boolean[] merge, final Boolean[] expected) {
    BooleanBlockIndex blockIndex = new BooleanBlockIndex(initial[0], initial[1], initial[2]);
    assertTrue(blockIndex.merge(new BooleanBlockIndex(merge[0], merge[1], merge[2])));
    assertEquals(expected[0], blockIndex.hasTrue());
    assertEquals(expected[1], blockIndex.hasFalse());
    assertEquals(expected[2], blockIndex.hasNull());
  }

  @Test
  public void T_getBinarySize_1() {
    BooleanBlockIndex blockIndex = new BooleanBlockIndex(true, true, true);
    assertEquals(Byte.BYTES, blockIndex.getBinarySize());
  }

  public static Stream<Arguments> D_binary_1() {
    return Stream.of(
        arguments(true, true, true),
        arguments(true, true, false),
        arguments(true, false, true),
        arguments(true, false, false),
        arguments(false, false, false),
        arguments(false, false, true),
        arguments(false, true, false),
        arguments(false, true, true));
  }

  @ParameterizedTest
  @MethodSource("D_binary_1")
  public void T_binary_0(final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
    BooleanBlockIndex blockIndex = new BooleanBlockIndex(hasTrue, hasFalse, hasNull);
    byte[] binary = blockIndex.toBinary();
    assertEquals(binary.length, blockIndex.getBinarySize());
    BooleanBlockIndex blockIndex2 = new BooleanBlockIndex();
    assertFalse(blockIndex2.hasTrue());
    assertFalse(blockIndex2.hasFalse());
    assertFalse(blockIndex2.hasNull());
    blockIndex2.setFromBinary(binary, 0, binary.length);
    assertEquals(hasTrue, blockIndex2.hasTrue());
    assertEquals(hasFalse, blockIndex2.hasFalse());
    assertEquals(hasNull, blockIndex2.hasNull());
  }

  public static Stream<Arguments> D_canBlockSkip_1() {
    return Stream.of(
        arguments(new BooleanBlockIndex(true, false, false), new BooleanFilter(true), false),
        arguments(new BooleanBlockIndex(true, false, false), new BooleanFilter(false), true),
        arguments(new BooleanBlockIndex(false, true, false), new BooleanFilter(true), true),
        arguments(new BooleanBlockIndex(false, true, false), new BooleanFilter(false), false),
        arguments(new BooleanBlockIndex(false, false, true), new BooleanFilter(true), true),
        arguments(new BooleanBlockIndex(false, false, true), new BooleanFilter(false), true),
        arguments(new BooleanBlockIndex(true, true, false), new BooleanFilter(true), false),
        arguments(new BooleanBlockIndex(true, true, false), new BooleanFilter(false), false),
        arguments(new BooleanBlockIndex(true, false, true), new BooleanFilter(true), false),
        arguments(new BooleanBlockIndex(true, false, true), new BooleanFilter(false), true),
        arguments(new BooleanBlockIndex(false, true, true), new BooleanFilter(true), true),
        arguments(new BooleanBlockIndex(false, true, true), new BooleanFilter(false), false),
        arguments(new BooleanBlockIndex(true, true, true), new BooleanFilter(true), false),
        arguments(new BooleanBlockIndex(true, true, true), new BooleanFilter(false), false));
  }

  @ParameterizedTest
  @MethodSource("D_canBlockSkip_1")
  public void T_canBlockSkip_1(
      final IBlockIndex blockIndex, final IFilter filter, final boolean result) {
    if (result) {
      assertEquals(result, blockIndex.getBlockSpreadIndex(filter).isEmpty());
    } else {
      assertNull(blockIndex.getBlockSpreadIndex(filter));
    }
  }

  public static Stream<Arguments> D_bitFlags_1() {
    return Stream.of(
        // bitFlags, hasTrue, hasFalse, hasNull
        arguments((byte) 0b111, true, true, true),
        arguments((byte) 0b110, false, true, true),
        arguments((byte) 0b101, true, false, true),
        arguments((byte) 0b100, false, false, true),
        arguments((byte) 0b000, false, false, false),
        arguments((byte) 0b001, true, false, false),
        arguments((byte) 0b010, false, true, false),
        arguments((byte) 0b011, true, true, false));
  }

  @ParameterizedTest
  @MethodSource("D_bitFlags_1")
  public void T_bitFlags_1(
      final byte flags, final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
    BooleanBlockIndex.BitFlags bitFlags = new BooleanBlockIndex.BitFlags(flags);
    assertEquals(hasTrue, bitFlags.hasTrue());
    assertEquals(hasFalse, bitFlags.hasFalse());
    assertEquals(hasNull, bitFlags.hasNull());
    assertEquals(flags, bitFlags.getBitFlags());
  }

  public static Stream<Arguments> D_bitFlags_2() {
    return Stream.of(
        // bitFlags, hasTrue, hasFalse, hasNull
        arguments((byte) 0b111, true, true, true),
        arguments((byte) 0b110, false, true, true),
        arguments((byte) 0b101, true, false, true),
        arguments((byte) 0b100, false, false, true),
        arguments((byte) 0b000, false, false, false),
        arguments((byte) 0b001, true, false, false),
        arguments((byte) 0b010, false, true, false),
        arguments((byte) 0b011, true, true, false));
  }

  @ParameterizedTest
  @MethodSource("D_bitFlags_2")
  public void T_bitFlags_2(
      final byte flags, final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
    BooleanBlockIndex.BitFlags bitFlags =
        new BooleanBlockIndex.BitFlags(hasTrue, hasFalse, hasNull);
    assertEquals(hasTrue, bitFlags.hasTrue());
    assertEquals(hasFalse, bitFlags.hasFalse());
    assertEquals(hasNull, bitFlags.hasNull());
    assertEquals(flags, bitFlags.getBitFlags());
  }
}
