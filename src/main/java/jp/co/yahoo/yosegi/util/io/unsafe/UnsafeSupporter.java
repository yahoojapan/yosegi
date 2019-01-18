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

package jp.co.yahoo.yosegi.util.io.unsafe;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;

public class UnsafeSupporter implements IWriteSupporter,IReadSupporter {

  private final byte[] binary;
  private final int start;
  private final int length;

  private int currentOffset;

  /**
   * Set a byte array to create a new object.
   */
  public UnsafeSupporter( final byte[] binary , final int start , final int length ) {
    this.binary = binary;
    this.start = start;
    this.length = length;
    currentOffset = ARRAY_BYTE_BASE_OFFSET + start;
  }

  @Override
  public void putByte( final byte value ) {
    UnsafeUtil.putByte( binary , currentOffset , value );
    currentOffset += Byte.BYTES;
  }

  @Override
  public byte getByte() {
    byte result = UnsafeUtil.getByte( binary , currentOffset );
    currentOffset += Byte.BYTES;
    return result;
  }

  @Override
  public void putShort( final short value ) {
    UnsafeUtil.putShort( binary , currentOffset , value );
    currentOffset += Short.BYTES;
  }

  @Override
  public short getShort() {
    short result = UnsafeUtil.getShort( binary , currentOffset );
    currentOffset += Short.BYTES;
    return result;
  }

  @Override
  public void putInt( final int value ) {
    UnsafeUtil.putInt( binary , currentOffset , value );
    currentOffset += Integer.BYTES;
  }

  @Override
  public int getInt() {
    int result = UnsafeUtil.getInt( binary , currentOffset );
    currentOffset += Integer.BYTES;
    return result;
  }

  @Override
  public void putLong( final long value ) {
    UnsafeUtil.putLong( binary , currentOffset , value );
    currentOffset += Long.BYTES;
  }

  @Override
  public long getLong() {
    long result = UnsafeUtil.getLong( binary , currentOffset );
    currentOffset += Long.BYTES;
    return result;
  }

  @Override
  public void putFloat( final float value ) {
    UnsafeUtil.putFloat( binary , currentOffset , value );
    currentOffset += Float.BYTES;
  }

  @Override
  public float getFloat() {
    float result = UnsafeUtil.getFloat( binary , currentOffset );
    currentOffset += Float.BYTES;
    return result;
  }

  @Override
  public void putDouble( final double value ) {
    UnsafeUtil.putDouble( binary , currentOffset , value );
    currentOffset += Double.BYTES;
  }

  @Override
  public double getDouble() {
    double result = UnsafeUtil.getDouble( binary , currentOffset );
    currentOffset += Double.BYTES;
    return result;
  }

}
