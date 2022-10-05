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

import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;

public class ArrowDictionaryBooleanLoader implements IDictionaryLoader<ValueVector> {

  private final BitVector vector;
  private final int loadSize;
  private boolean[] dic;
  private boolean[] dicIsNullArray;

  /**
   * A loader that holds elements dictionary.
   */
  public ArrowDictionaryBooleanLoader( final ValueVector vector , final int loadSize ) {
    this.vector = (BitVector)vector;
    this.vector.allocateNew( loadSize );
    this.vector.setValueCount( loadSize );
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
    dic = new boolean[dictionarySize];
    dicIsNullArray = new boolean[dictionarySize];
  }

  @Override
  public void setDictionaryIndex( final int index , final int dicIndex ) throws IOException {
    if ( dicIsNullArray[dicIndex] ) {
      setNull( index );
    } else {
      if ( dic[dicIndex] ) {
        vector.setSafe( index , 1 );
      } else {
        vector.setSafe( index , 0 );
      }
    }
  }

  @Override
  public void setNullToDic( final int index ) throws IOException {
    dicIsNullArray[index] = true;
  }

  @Override
  public void setBooleanToDic( final int index , final boolean value ) throws IOException {
    dic[index] = value;
  }

  @Override
  public void setBytesToDic(int index, byte[] value, int start, int length) throws IOException {
    try {
      setBooleanToDic( index , new BytesObj( value , start , length ).getBoolean() );
    } catch ( Exception ex ) {
      setNullToDic( index );
    }
  }

  @Override
  public void setStringToDic(int index, String value) throws IOException {
    try {
      setBooleanToDic( index , new StringObj( value ).getBoolean() );
    } catch ( Exception ex ) {
      setNullToDic( index );
    }
  }

}
