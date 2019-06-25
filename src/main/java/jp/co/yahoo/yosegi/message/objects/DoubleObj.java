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

  private Double value;

  public DoubleObj() {
    value = Double.valueOf(0);
  }

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
    if ( value < Float.MIN_VALUE || Float.MAX_VALUE < value ) {
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
  public void setString( final String data ) throws IOException {
    value = Double.parseDouble( data );
  }

  @Override
  public void setBytes( final byte[] data ) throws IOException {
    setBytes( data , 0 , data.length );
  }

  @Override
  public void setBytes(
      final byte[] data ,
      final int start ,
      final int length ) throws IOException {
    value = Double.parseDouble( new String( data , start , length ) );
  }

  @Override
  public void setByte( final byte data ) throws IOException {
    setInt( (int)data );
  }

  @Override
  public void setShort( final short data ) throws IOException {
    value = Short.valueOf( data ).doubleValue();
  }

  @Override
  public void setInt( final int data ) throws IOException {
    value = Integer.valueOf( data ).doubleValue();
  }

  @Override
  public void setLong( final long data ) throws IOException {
    value = Long.valueOf( data ).doubleValue();
  }

  @Override
  public void setFloat( final float data ) throws IOException {
    value = Float.valueOf( data ).doubleValue();
  }

  @Override
  public void setDouble( final double data ) throws IOException {
    value = data;
  }

  @Override
  public void setBoolean( final boolean data ) throws IOException {
    if ( data ) {
      value = Double.valueOf( 1 );
    } else {
      value = Double.valueOf( 0 );
    }
  }

  @Override
  public void set( final PrimitiveObject data ) throws IOException {
    value = data.getDouble();
  }

  @Override
  public void clear() throws IOException {
    value = Double.valueOf(0);
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
