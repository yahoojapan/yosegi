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

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.expression.AllExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IndexFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;
import java.util.Objects;

public class DynamicArrowLoader extends ArrowLoader {
  /**
   * FileReader and Arrow memory allocators are set and initialized.
   */
  public DynamicArrowLoader(
      final IRootMemoryAllocator rootMemoryAllocator,
      final YosegiReader reader,
      final BufferAllocator allocator) {
    super(rootMemoryAllocator, reader, allocator);
  }

  @Override
  public ValueVector next() throws IOException {
    rootVector.clear();
    Spread spread = reader.next();
    IMemoryAllocator memoryAllocator =
        rootMemoryAllocator.create( allocator , rootVector , spread.size() );
    IExpressionIndex index = new AllExpressionIndex( spread.size() );
    if ( Objects.nonNull(node) ) {
      index = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
      if ( index.size() == 0 ) {
        memoryAllocator.setValueCount( 0 );
        return rootVector;
      }
    }
    memoryAllocator.setValueCount( index.size() );
    for ( IColumn column : spread.getListColumn() ) {
      IMemoryAllocator childMemoryAllocator =
          memoryAllocator.getChild( column.getColumnName() , column.getColumnType() );
      column.setPrimitiveObjectArray( index , 0 , index.size() , childMemoryAllocator );
      childMemoryAllocator.setValueCount( index.size() );
    }
    return rootVector;
  }
}

