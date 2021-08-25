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

public interface ISequentialLoader<T> extends ILoader<T> {

  @Override
  default LoadType getLoaderType() {
    return LoadType.SEQUENTIAL;
  }

  default void setBoolean( final int index , final boolean value ) throws IOException {
    setNull( index );
  }

  default void setByte( final int index , final byte value ) throws IOException {
    setNull( index );
  }

  default void setShort( final int index , final short value ) throws IOException {
    setNull( index );
  }

  default void setInteger( final int index , final int value ) throws IOException {
    setNull( index );
  }

  default void setLong( final int index , final long value ) throws IOException {
    setNull( index );
  }

  default void setFloat( final int index , final float value ) throws IOException {
    setNull( index );
  }

  default void setDouble( final int index , final double value ) throws IOException {
    setNull( index );
  }

  default void setBytes( final int index , final byte[] value ) throws IOException {
    setBytes( index , value , 0 , value.length );
  }

  default void setBytes(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    setNull( index );
  }

  default void setString( final int index , final String value ) throws IOException {
    setNull( index );
  }

  default void setString( final int index , final char[] value ) throws IOException {
    setString( index , new String( value ) );
  }

  default void setString(
      final int index ,
      final char[] value ,
      final int start ,
      final int length ) throws IOException {
    setString( index , new String( value , start , length ) );
  }

}
