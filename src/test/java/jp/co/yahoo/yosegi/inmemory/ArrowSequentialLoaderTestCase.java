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

import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.spread.column.*;

import org.apache.arrow.vector.*
;

import java.io.IOException;

public final class ArrowSequentialLoaderTestCase {

  private ArrowSequentialLoaderTestCase() {}

  public static IColumn createVectorFromString(ISequentialLoader<ValueVector> loader, String[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setString(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromBytes(ISequentialLoader<ValueVector> loader, byte[][] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setBytes(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromBoolean(ISequentialLoader<ValueVector> loader, boolean[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setBoolean(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromByte(ISequentialLoader<ValueVector> loader, byte[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setByte(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromShort(ISequentialLoader<ValueVector> loader, short[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setShort(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromInteger(ISequentialLoader<ValueVector> loader, int[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setInteger(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromLong(ISequentialLoader<ValueVector> loader, long[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setLong(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromFloat(ISequentialLoader<ValueVector> loader, float[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setFloat(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromDouble(ISequentialLoader<ValueVector> loader, double[] data, boolean[] isNullArray) throws IOException {
    for (int i = 0; i < loader.getLoadSize(); i++) {
      if (isNullArray[i]) {
        loader.setNull(i);
      } else {
        loader.setDouble(i, data[i]);
      }
    }
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }
}
