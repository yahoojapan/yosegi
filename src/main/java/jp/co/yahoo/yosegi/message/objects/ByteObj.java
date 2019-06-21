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

public class ByteObj implements PrimitiveObject {

  private Byte value;

  public ByteObj() {
    value = Byte.valueOf( (byte)0 );
  }

  public ByteObj( final byte value ) {
    this.value = Byte.valueOf( value );
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
    return value;
  }

  @Override
  public short getShort() throws IOException {
    return value.shortValue();
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
    return ! ( value.equals( Byte.valueOf( (byte)0 ) ) );
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.BYTE;
  }

  @Override
  public int getObjectSize() {
    return Byte.BYTES;
  }

}
