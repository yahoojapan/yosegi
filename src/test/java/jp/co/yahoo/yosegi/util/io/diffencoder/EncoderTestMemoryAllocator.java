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

package jp.co.yahoo.yosegi.util.io.diffencoder;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import java.io.IOException;

public class EncoderTestMemoryAllocator implements IMemoryAllocator {

  private final long[] longArray;
  private final boolean[] isNullArray;

  public EncoderTestMemoryAllocator( final int arraySize ) {
    longArray = new long[arraySize];
    isNullArray = new boolean[arraySize];
  }

  public long[] getLongArray() {
    return longArray;
  }

  public boolean[] getIsNullArray() {
    return isNullArray;
  }

  @Override
  public void setNull( final int index ) {
    isNullArray[index] = true;
  }

  @Override
  public void setByte( final int index , final byte value ) throws IOException {
    longArray[index] = (long)value;
  }

  @Override
  public void setShort( final int index , final short value ) throws IOException {
    longArray[index] = (long)value;
  }

  @Override
  public void setInteger( final int index , final int value ) throws IOException {
    longArray[index] = (long)value;
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException {
    longArray[index] = value;
  }

}
