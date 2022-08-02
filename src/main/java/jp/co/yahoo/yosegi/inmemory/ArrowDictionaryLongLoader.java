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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;

public class ArrowDictionaryLongLoader implements IDictionaryLoader<ValueVector> {

  private final BigIntVector vector;
  private final int loadSize;
  private long[] dic;
  private boolean[] dicIsNullArray;

  /**
   * A loader that holds elements dictionary.
   */
  public ArrowDictionaryLongLoader( final ValueVector vector , final int loadSize ) {
    this.vector = (BigIntVector)vector;
    vector.allocateNew();
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public ValueVector build() throws IOException {
    return vector;
  }

  @Override
  public void finish() throws IOException {
  }

  @Override
  public void setNull( final int index ) throws IOException {
    vector.setNull( index );
  }

  @Override
  public void createDictionary( final int dictionarySize ) throws IOException {
    dic = new long[dictionarySize];
    dicIsNullArray = new boolean[dictionarySize];
  }

  @Override
  public void setDictionaryIndex( final int index , final int dicIndex ) throws IOException {
    if ( dicIsNullArray[dicIndex] ) {
      setNull( index );
    } else {
      vector.setSafe( index , dic[dicIndex] );
    }
  }

  @Override
  public void setNullToDic( final int index ) throws IOException {
    dicIsNullArray[index] = true;
  }

  @Override
  public void setByteToDic( final int index , final byte value ) throws IOException {
    setLongToDic( index , value );
  }

  @Override
  public void setShortToDic( final int index , final short value ) throws IOException {
    setLongToDic( index , value );
  }

  @Override
  public void setIntegerToDic( final int index , final int value ) throws IOException {
    setLongToDic( index , value );
  }

  @Override
  public void setLongToDic( final int index , final long value ) throws IOException {
    dic[index] = value;
  }

}
