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

package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

import java.io.IOException;

public interface ICellManager<T> {

  void add( final T cell , final int index );

  ICell get( final int index , final ICell defaultCell );

  int size();

  void clear();

  PrimitiveObject[] getPrimitiveObjectArray(
      final int start , final int length );

  void setPrimitiveObjectArray(
      final int start ,
      final int length ,
      final IMemoryAllocator allocator );

  default boolean isDictionary() {
    return false;
  }

  default int getDictionarySize() {
    throw new UnsupportedOperationException( "This method only supports dictionary columns." );
  }

  default boolean[] getDictionaryIsNullArray() {
    throw new UnsupportedOperationException( "This method only supports dictionary columns." );
  }

  default int[] getDictionaryIndexArray() {
    throw new UnsupportedOperationException( "This method only supports dictionary columns." );
  }

  default PrimitiveObject[] getDictionaryArray() {
    throw new UnsupportedOperationException( "This method only supports dictionary columns." );
  }

}
