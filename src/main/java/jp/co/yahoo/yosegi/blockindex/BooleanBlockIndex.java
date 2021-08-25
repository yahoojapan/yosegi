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
import jp.co.yahoo.yosegi.spread.column.filter.FilterType;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BooleanBlockIndex implements IBlockIndex {

  private boolean hasTrue;
  private boolean hasFalse;
  private boolean hasNull;

  /**
   * BooleanBlockIndex constructor.
   */
  public BooleanBlockIndex() {
    hasTrue = false;
    hasFalse = false;
    hasNull = false;
  }

  /**
   * BooleanBlockIndex constructor.
   * @param hasTrue true when true values exist.
   * @param hasFalse true when false values exist.
   * @param hasNull true when null values exist.
   */
  public BooleanBlockIndex(final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
    this.hasTrue = hasTrue;
    this.hasFalse = hasFalse;
    this.hasNull = hasNull;
  }

  @Override
  public IBlockIndex clone() {
    return new BooleanBlockIndex(hasTrue, hasFalse, hasNull);
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.BOOLEAN;
  }

  @Override
  public boolean merge(final IBlockIndex blockIndex) {
    if (!(blockIndex instanceof BooleanBlockIndex)) {
      return false;
    }
    BooleanBlockIndex booleanBlockIndex = (BooleanBlockIndex) blockIndex;
    if (booleanBlockIndex.hasTrue()) {
      hasTrue = true;
    }
    if (booleanBlockIndex.hasFalse()) {
      hasFalse = true;
    }
    if (booleanBlockIndex.hasNull()) {
      hasNull = true;
    }
    return true;
  }

  @Override
  public int getBinarySize() {
    return BitFlags.LENGTH;
  }

  @Override
  public byte[] toBinary() {
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap(result);
    BitFlags bitFlags = new BitFlags(hasTrue, hasFalse, hasNull);
    wrapBuffer.put(bitFlags.getBitFlags());
    return result;
  }

  @Override
  public void setFromBinary(final byte[] buffer, final int start, final int length) {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(buffer, start, length);
    byte flags = wrapBuffer.get();
    BitFlags bitFlags = new BitFlags(flags);
    hasTrue = bitFlags.hasTrue();
    hasFalse = bitFlags.hasFalse();
    hasNull = bitFlags.hasNull();
  }

  @Override
  public List<Integer> getBlockSpreadIndex(final IFilter filter) {
    if (filter.getFilterType() == FilterType.BOOLEAN) {
      BooleanFilter booleanFilter = (BooleanFilter) filter;
      if (booleanFilter.getFlag()) {
        if (!hasTrue) {
          return new ArrayList<Integer>();
        }
      } else {
        if (!hasFalse) {
          return new ArrayList<Integer>();
        }
      }
    }
    return null;
  }

  @Override
  public IBlockIndex getNewInstance() {
    return new BooleanBlockIndex();
  }

  public boolean hasTrue() {
    return hasTrue;
  }

  public boolean hasFalse() {
    return hasFalse;
  }

  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public String toString() {
    return String.format(
        "%s hasTrue=%b hasFalse=%b hasNull=%b",
        this.getClass().getName(), hasTrue, hasFalse, hasNull);
  }

  public static class BitFlags {

    public static final int LENGTH = Byte.BYTES;

    private static final byte HAS_TRUE = 1;
    private static final byte HAS_FALSE = 1 << 1;
    private static final byte HAS_NULL = 1 << 2;

    private boolean hasTrue;
    private boolean hasFalse;
    private boolean hasNull;

    // bitFlags: 001:hasTrue, 010:hasFalse, 100:hasNull
    private byte bitFlags;

    /**
     * BitFlags constructor.
     * @param bitFlags 001:hasTrue, 010:hasFalse, 100:hasNull.
     */
    public BitFlags(final byte bitFlags) {
      this.bitFlags = bitFlags;
      hasTrue = (bitFlags & HAS_TRUE) != 0;
      hasFalse = (bitFlags & HAS_FALSE) != 0;
      hasNull = (bitFlags & HAS_NULL) != 0;
    }

    /**
     * BitFlags constructor.
     * @param hasTrue true when true values exist.
     * @param hasFalse true when false values exist.
     * @param hasNull true when null values exist.
     */
    public BitFlags(final boolean hasTrue, final boolean hasFalse, final boolean hasNull) {
      this.hasTrue = hasTrue;
      this.hasFalse = hasFalse;
      this.hasNull = hasNull;
      bitFlags = hasTrue ? HAS_TRUE : 0;
      bitFlags |= hasFalse ? HAS_FALSE : 0;
      bitFlags |= hasNull ? HAS_NULL : 0;
    }

    public byte getBitFlags() {
      return bitFlags;
    }

    public boolean hasTrue() {
      return hasTrue;
    }

    public boolean hasFalse() {
      return hasFalse;
    }

    public boolean hasNull() {
      return hasNull;
    }
  }
}
