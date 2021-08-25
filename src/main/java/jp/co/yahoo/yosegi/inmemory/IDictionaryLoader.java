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

public interface IDictionaryLoader<T> extends ILoader<T> {

  @Override
  default LoadType getLoaderType() {
    return LoadType.DICTIONARY;
  }

  void createDictionary( final int dictionarySize );

  void setDictionaryIndex( final int index , final int dicIndex );

  void setNullToDic( final int index ) throws IOException;

  default void setBooleanToDic( final int index , final boolean value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setBoolean()" );
  }

  default void setByteToDic( final int index , final byte value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setByte()" );
  }

  default void setShortToDic( final int index , final short value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setShort()" );
  }

  default void setIntegerToDic( final int index , final int value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setInteger()" );
  }

  default void setLongToDic( final int index , final long value ) throws IOException {
    setNull( index );
  }

  default void setFloatToDic( final int index , final float value ) throws IOException {
    setNull( index );
  }

  default void setDoubleToDic( final int index , final double value ) throws IOException {
    setNull( index );
  }

  default void setBytesToDic( final int index , final byte[] value ) throws IOException {
    setBytesToDic( index , value , 0 , value.length );
  }

  default void setBytesToDic(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    setNull( index );
  }

  default void setStringToDic( final int index , final String value ) throws IOException {
    setNull( index );
  }

  default void setStringToDic( final int index , final char[] value ) throws IOException {
    setStringToDic( index , new String( value ) );
  }

  default void setStringToDic(
      final int index ,
      final char[] value ,
      final int start ,
      final int length ) throws IOException {
    setStringToDic( index , new String( value , start , length ) );
  }

}
