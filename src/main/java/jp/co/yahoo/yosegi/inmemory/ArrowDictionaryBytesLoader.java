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

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import jp.co.yahoo.yosegi.message.objects.Utf8BytesLinkObj;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

import java.io.IOException;

public class ArrowDictionaryBytesLoader implements IDictionaryLoader<ValueVector> {

  private final VarBinaryVector vector;
  private final int loadSize;
  private Utf8BytesLinkObj[] dic;

  /**
   * A loader that holds elements dictionary.
   */
  public ArrowDictionaryBytesLoader( final ValueVector vector , final int loadSize ) {
    this.vector = (VarBinaryVector)vector;
    this.vector.allocateNew();
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
    dic = new Utf8BytesLinkObj[dictionarySize];
  }

  @Override
  public void setDictionaryIndex( final int index , final int dicIndex ) throws IOException {
    if ( dic[dicIndex] == null ) {
      setNull( index );
    } else {
      vector.setSafe(
          index ,
          dic[dicIndex].getLinkBytes() ,
          dic[dicIndex].getStart() ,
          dic[dicIndex].getLength() );
    }
  }

  @Override
  public void setNullToDic( final int index ) throws IOException {
    dic[index] = null;
  }

  @Override
  public void setBytesToDic(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    dic[index] = new Utf8BytesLinkObj( value , start , length );
  }

  @Override
  public void setStringToDic( final int index , final String value ) throws IOException {
    setBytesToDic( index , new StringObj( value ).getBytes() );
  }

  @Override
  public void setBooleanToDic( final int index , final boolean value ) throws IOException {
    setBytesToDic( index , new BooleanObj( value ).getBytes() );
  }

  @Override
  public void setByteToDic( final int index , final byte value ) throws IOException {
    setBytesToDic( index , new ByteObj( value ).getBytes() );
  }

  @Override
  public void setShortToDic( final int index , final short value ) throws IOException {
    setBytesToDic( index , new ShortObj( value ).getBytes() );
  }

  @Override
  public void setIntegerToDic( final int index , final int value ) throws IOException {
    setBytesToDic( index , new IntegerObj( value ).getBytes() );
  }

  @Override
  public void setLongToDic( final int index , final long value ) throws IOException {
    setBytesToDic( index , new LongObj( value ).getBytes() );
  }

  @Override
  public void setFloatToDic( final int index , final float value ) throws IOException {
    setBytesToDic( index , new FloatObj( value ).getBytes() );
  }

  @Override
  public void setDoubleToDic( final int index , final double value ) throws IOException {
    setBytesToDic( index , new DoubleObj( value ).getBytes() );
  }

}
