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

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import jp.co.yahoo.yosegi.spread.column.*;

import org.apache.arrow.vector.*
;

import java.io.IOException;

public final class ArrowConstLoaderTestCase {

  private ArrowConstLoaderTestCase() {}

  public static IColumn createVectorFromString(IConstLoader<ValueVector> loader, String data) throws IOException {
    loader.setConstFromString(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromBytes(IConstLoader<ValueVector> loader, byte[] data) throws IOException {
    loader.setConstFromBytes(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromBoolean(IConstLoader<ValueVector> loader, boolean data) throws IOException {
    loader.setConstFromBoolean(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromByte(IConstLoader<ValueVector> loader, byte data) throws IOException {
    loader.setConstFromByte(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromShort(IConstLoader<ValueVector> loader, short data) throws IOException {
    loader.setConstFromShort(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromInteger(IConstLoader<ValueVector> loader, int data) throws IOException {
    loader.setConstFromInteger(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromLong(IConstLoader<ValueVector> loader, long data) throws IOException {
    loader.setConstFromLong(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromFloat(IConstLoader<ValueVector> loader, float data) throws IOException {
    loader.setConstFromFloat(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }

  public static IColumn createVectorFromDouble(IConstLoader<ValueVector> loader, double data) throws IOException {
    loader.setConstFromDouble(data);
    loader.finish();
    return ArrowColumnFactory.convert( "vector" , loader.build() );
  }
}
