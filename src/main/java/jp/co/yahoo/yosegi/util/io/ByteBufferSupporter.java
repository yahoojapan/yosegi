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

package jp.co.yahoo.yosegi.util.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteBufferSupporter implements IWriteSupporter,IReadSupporter {

  private final ByteBuffer buffer;

  public ByteBufferSupporter( final byte[] binary , final int start ,
      final int length , final ByteOrder byteOrder ) {
    buffer = ByteBuffer.wrap( binary , start , length ).order( byteOrder );
  }

  @Override
  public void putByte( final byte value ) {
    buffer.put( value );
  }

  @Override
  public byte getByte() {
    return buffer.get();
  }

  @Override
  public void putShort( final short value ) {
    buffer.putShort( value );
  }
 
  @Override
  public short getShort() {
    return buffer.getShort();
  }

  @Override
  public void putInt( final int value ) {
    buffer.putInt( value );
  }

  @Override
  public int getInt() {
    return buffer.getInt();
  }

  @Override
  public void putLong( final long value ) {
    buffer.putLong( value );
  }
 
  @Override
  public long getLong() {
    return buffer.getLong();
  }

  @Override
  public void putFloat( final float value ) {
    buffer.putFloat( value );
  }

  @Override
  public float getFloat() {
    return buffer.getFloat();
  }

  @Override
  public void putDouble( final double value ) {
    buffer.putDouble( value );
  }

  @Override
  public double getDouble() {
    return buffer.getDouble();
  }

}
