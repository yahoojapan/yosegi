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

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.spread.column.*;

import org.apache.arrow.vector.*
;

import java.io.IOException;

public final class ArrowDictionaryLoaderTestCase {

  private ArrowDictionaryLoaderTestCase() {}

  public static IColumn createVectorFromString(IDictionaryLoader<ValueVector> loader, int[] ids, String[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setStringToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromBytes(IDictionaryLoader<ValueVector> loader, int[] ids, byte[][] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setBytesToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromBoolean(IDictionaryLoader<ValueVector> loader, int[] ids, boolean[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setBooleanToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromByte(IDictionaryLoader<ValueVector> loader, int[] ids, byte[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setByteToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromShort(IDictionaryLoader<ValueVector> loader, int[] ids, short[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setShortToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromInteger(IDictionaryLoader<ValueVector> loader, int[] ids, int[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setIntegerToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromLong(IDictionaryLoader<ValueVector> loader, int[] ids, long[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setLongToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromFloat(IDictionaryLoader<ValueVector> loader, int[] ids, float[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setFloatToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromDouble(IDictionaryLoader<ValueVector> loader, int[] ids, double[] dic, boolean[] isNullArray) throws IOException {
    loader.createDictionary(dic.length);
    for (int i = 0; i < dic.length; i++) {
      loader.setDoubleToDic(i, dic[i]);
    }
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDictionaryIndex(i, ids[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }
}
