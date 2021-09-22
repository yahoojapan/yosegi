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

package jp.co.yahoo.yosegi.util.io.diffencoder;

import jp.co.yahoo.yosegi.inmemory.IDictionary;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import java.io.IOException;
import java.nio.ByteOrder;

public interface INumEncoder {

  int calcBinarySize( final int rows );

  void toBinary(
      final long[] longArray ,
      final byte[] buffer ,
      final int start ,
      final int rows,
      final ByteOrder order ) throws IOException;

  void toBinary(
      final Long[] longArray ,
      final byte[] buffer ,
      final int start ,
      final int rows,
      final ByteOrder order ) throws IOException;

  void setDictionary(
      final byte[] buffer ,
      final int start ,
      final int rows ,
      final ByteOrder order ,
      final IDictionary dic ) throws IOException;

  void setSequentialLoader(
          final byte[] buffer,
          final int start,
          final int rows,
          final boolean[] isNullArray,
          final ByteOrder order,
          final ISequentialLoader loader,
          final int startIndex
  ) throws IOException;

  void setDictionaryLoader(
          final byte[] buffer,
          final int start,
          final int rows,
          final boolean[] isNullArray,
          final ByteOrder order,
          final IDictionaryLoader loader,
          final int startIndex,
          final int[] loadIndexArray
  ) throws IOException;
}
