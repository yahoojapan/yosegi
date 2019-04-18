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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import java.io.IOException;
import java.util.List;

public class InMemoryStorageLoader implements AutoCloseable {

  private final YosegiReader reader;

  public InMemoryStorageLoader( final YosegiReader reader ) {
    this.reader = reader;
  }

  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  /**
   * Read the next Spread.
   */
  public void next( final IMemoryAllocator allocator ) throws IOException {
    List<ColumnBinary> columnBinaryList = reader.nextRaw();
    if ( columnBinaryList == null ) {
      return;
    }
    int maxValueCount = 0;
    for ( ColumnBinary columnBinary : columnBinaryList ) {
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
      IMemoryAllocator childAllocator =
          allocator.getChild( columnBinary.columnName , columnBinary.columnType );
      maker.loadInMemoryStorage( columnBinary , childAllocator );
      if ( maxValueCount < childAllocator.getValueCount() ) {
        maxValueCount = childAllocator.getValueCount();
      }
    }
    allocator.setValueCount( maxValueCount );
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
