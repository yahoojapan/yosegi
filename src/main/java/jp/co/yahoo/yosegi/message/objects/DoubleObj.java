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

public class DoubleObj implements PrimitiveObject {

  private static final double BYTE_MIN = Double.valueOf( Byte.MIN_VALUE );
  private static final double BYTE_MAX = Double.valueOf( Byte.MAX_VALUE );

  private static final double SHORT_MIN = Double.valueOf( Short.MIN_VALUE );
  private static final double SHORT_MAX = Double.valueOf( Short.MAX_VALUE );

  private static final double INTEGER_MIN = Double.valueOf( Integer.MIN_VALUE );
  private static final double INTEGER_MAX = Double.valueOf( Integer.MAX_VALUE );

  private static final double LONG_MIN = Double.valueOf( Long.MIN_VALUE );
  private static final double LONG_MAX = Double.valueOf( Long.MAX_VALUE );

  private final Double value;

  public DoubleObj( final double value ) {
    this.value = value;
  }

  @Override
  public Object get() throws IOException {
    return value;
  }

  @Override
  public String getString() throws IOException {
    return Double.toString( value );
  }

  @Override
  public byte[] getBytes() throws IOException {
    return value.toString().getBytes( "UTF-8" );
  }

  @Override
  public byte getByte() throws IOException {
    if ( value < BYTE_MIN || BYTE_MAX < value ) {
      throw new NumberFormatException(
          "Can not down cast. double to byte. double value is " + value );
    }
    return value.byteValue();
  }

  @Override
  public short getShort() throws IOException {
    if ( value < SHORT_MIN || SHORT_MAX < value ) {
      throw new NumberFormatException(
          "Can not down cast. double to short. double value is " + value );
    }
    return value.shortValue();
  }

  @Override
  public int getInt() throws IOException {
    if ( value < INTEGER_MIN || INTEGER_MAX < value ) {
      throw new NumberFormatException(
          "Can not down cast. double to integer. double value is " + value );
    }
    return value.intValue();
  }

  @Override
  public long getLong() throws IOException {
    // Same as Long.MAX_VALUE up to 1024
    // Same as Long.MIN_VALUE up to 1024
    if ( value < LONG_MIN || LONG_MAX < value ) {
      throw new NumberFormatException(
          "Can not down cast. double to long. double value is " + value );
    }
    return value.longValue();
  }

  @Override
  public float getFloat() throws IOException {
    if ( value < -Float.MAX_VALUE || Float.MAX_VALUE < value ) {
      throw new NumberFormatException(
        "Can not down cast. double to float. double value is " + value );
    }
    return value.floatValue();
  }

  @Override
  public double getDouble() throws IOException {
    return value;
  }

  @Override
  public boolean getBoolean() throws IOException {
    return ! ( value.equals( Double.valueOf(0) ) );
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.DOUBLE;
  }

  @Override
  public int getObjectSize() {
    return Double.BYTES;
  }

}
