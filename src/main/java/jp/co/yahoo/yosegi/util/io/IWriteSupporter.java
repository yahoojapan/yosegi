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

public interface IWriteSupporter {

  default void putBoolean( final boolean value ) {
    putByte( value ? (byte)1 : (byte)0 );
  }

  default void putByte( final byte value ) {
    throw new UnsupportedOperationException( "Unsupported method putByte()" );
  }

  default void putShort( final short value ) {
    throw new UnsupportedOperationException( "Unsupported method putShort()" );
  }

  default void putInt( final int value ) {
    throw new UnsupportedOperationException( "Unsupported method putInt()" );
  }

  default void putLong( final long value ) {
    throw new UnsupportedOperationException( "Unsupported method putLong()" );
  }

  default void putFloat( final float value ) {
    throw new UnsupportedOperationException( "Unsupported method putFloat()" );
  }

  default void putDouble( final double value ) {
    throw new UnsupportedOperationException( "Unsupported method putDouble()" );
  }

}
