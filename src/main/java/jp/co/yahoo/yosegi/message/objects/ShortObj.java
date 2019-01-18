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

public class ShortObj implements PrimitiveObject {

  private Short value;

  public ShortObj() {
    value = Short.valueOf( (short)0 );
  }

  public ShortObj( final short value ) {
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
          "Can not down cast. long to byte. short value is " + value );
    }
    return value.byteValue();
  }

  @Override
  public short getShort() throws IOException {
    return value;
  }

  @Override
  public int getInt() throws IOException {
    return value.intValue();
  }

  @Override
  public long getLong() throws IOException {
    return value.longValue();
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
    return ! ( value.equals( Short.valueOf( (short)0 ) ) );
  }

  @Override
  public void setString( final String data ) throws IOException {
    value = Short.parseShort( data );
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
    value = Short.parseShort( new String( data , start , length ) );
  }

  @Override
  public void setByte( final byte data ) throws IOException {
    setInt( (int)data );
  }

  @Override
  public void setShort( final short data ) throws IOException {
    value = data;
  }

  @Override
  public void setInt( final int data ) throws IOException {
    value = Float.valueOf( data ).shortValue();
  }

  @Override
  public void setLong( final long data ) throws IOException {
    value = Long.valueOf( data ).shortValue();
  }

  @Override
  public void setFloat( final float data ) throws IOException {
    value = Float.valueOf( data ).shortValue();
  }

  @Override
  public void setDouble( final double data ) throws IOException {
    value = Double.valueOf( data ).shortValue();
  }

  @Override
  public void setBoolean( final boolean data ) throws IOException {
    if ( data ) {
      value = Short.valueOf( (short)1 );
    } else {
      value = Short.valueOf( (short)0 );
    }
  }

  @Override
  public void set( final PrimitiveObject data ) throws IOException {
    value = data.getShort();
  }

  @Override
  public void clear() throws IOException {
    value = Short.valueOf( (short)0 );
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.SHORT;
  }

  @Override
  public int getObjectSize() {
    return Short.BYTES;
  }

}
