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

package jp.co.yahoo.yosegi.message.objects;

import java.io.IOException;

public class LongObj implements PrimitiveObject {

  private Long value;

  public LongObj() {
    value = Long.valueOf(0);
  }

  public LongObj( final long value ) {
    this.value = value;
  }

  @Override
  public Object get() throws IOException {
    return value;
  }

  @Override
  public String getString() throws IOException {
    return value.toString();
  }

  @Override
  public byte[] getBytes() throws IOException {
    return value.toString().getBytes( "UTF-8" );
  }

  @Override
  public byte getByte() throws IOException {
    if ( value < Byte.MIN_VALUE || Byte.MAX_VALUE < value ) {
      throw new NumberFormatException(
          "Can not down cast. long to byte. long value is " + value );
    }
    return value.byteValue();
  }

  @Override
  public short getShort() throws IOException {
    if ( value < Short.MIN_VALUE || Short.MAX_VALUE < value ) {
      throw new NumberFormatException(
          "Can not down cast. long to short. long value is " + value );
    }
    return value.shortValue();
  }

  @Override
  public int getInt() throws IOException {
    if ( value < Integer.MIN_VALUE || Integer.MAX_VALUE < value ) {
      throw new NumberFormatException(
          "Can not down cast. long to int. long value is " + value );
    }
    return value.intValue();
  }

  @Override
  public long getLong() throws IOException {
    return value;
  }

  @Override
  public float getFloat() throws IOException {
    return value.floatValue();
  }

  @Override
  public double getDouble() throws IOException {
    return value.doubleValue();
  }

  @Override
  public boolean getBoolean() throws IOException {
    return ! ( value.equals( Long.valueOf(0) ) );
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.LONG;
  }

  @Override
  public int getObjectSize() {
    return Long.BYTES;
  }

}
