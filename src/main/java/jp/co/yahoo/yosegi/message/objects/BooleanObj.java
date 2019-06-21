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

public class BooleanObj implements PrimitiveObject {

  private Boolean value;
  private Integer numberValue;

  public BooleanObj() {
    value = false;
    numberValue = Integer.valueOf( 0 );
  }

  /**
   * Create a PrimitiveObject holding the input Boolean.
   */
  public BooleanObj( final boolean value ) {
    this.value = value;
    if ( value ) {
      numberValue = Integer.valueOf( 1 );
    } else {
      numberValue = Integer.valueOf( 0 );
    }
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
    return numberValue.byteValue();
  }

  @Override
  public short getShort() throws IOException {
    return numberValue.shortValue();
  }

  @Override
  public int getInt() throws IOException {
    return numberValue;
  }

  @Override
  public long getLong() throws IOException {
    return numberValue.longValue();
  }

  @Override
  public float getFloat() throws IOException {
    return numberValue.floatValue();
  }

  @Override
  public double getDouble() throws IOException {
    return numberValue.doubleValue();
  }

  @Override
  public boolean getBoolean() throws IOException {
    return value;
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.BOOLEAN;
  }

  @Override
  public int getObjectSize() {
    return Byte.BYTES;
  }

}
