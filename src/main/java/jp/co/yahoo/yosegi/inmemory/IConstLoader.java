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

package jp.co.yahoo.yosegi.inmemory;

import java.io.IOException;

public interface IConstLoader<T> extends ILoader<T> {

  @Override
  default LoadType getLoaderType() {
    return LoadType.CONST;
  }

  void setConstFromNull() throws IOException;

  default void setConstFromBoolean( final boolean value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromByte( final byte value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromShort( final short value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromInteger( final int value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromLong( final long value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromFloat( final float value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromDouble( final double value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromBytes( final byte[] value ) throws IOException {
    setConstFromBytes( value , 0 , value.length );
  }

  default void setConstFromBytes(
      final byte[] value , final int start , final int length ) throws IOException {
    setConstFromBytes( value , 0 , value.length );
  }

  default void setConstFromString( final String value ) throws IOException {
    setConstFromNull();
  }

  default void setConstFromString( final char[] value ) throws IOException {
    setConstFromString( new String( value ) );
  }

  default void setConstFromString(
      final char[] value ,
      final int start ,
      final int length ) throws IOException {
    setConstFromString( new String( value , start , length ) );
  }

}
