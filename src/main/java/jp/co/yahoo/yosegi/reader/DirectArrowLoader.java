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
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DirectArrowLoader implements IArrowLoader {

  private final StructVector rootVector;
  private final YosegiReader reader;
  private final BufferAllocator allocator;
  private final IRootMemoryAllocator rootMemoryAllocator;

  private IExpressionNode node;

  /**
   * FileReader and Arrow memory allocators are set and initialized.
   */
  public DirectArrowLoader(
      final IRootMemoryAllocator rootMemoryAllocator ,
      final YosegiReader reader ,
      final BufferAllocator allocator ) {
    this.reader = reader;
    this.allocator = allocator;
    this.rootMemoryAllocator = rootMemoryAllocator;
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    rootVector = new StructVector(
        "root" , allocator , new FieldType( true , Struct.INSTANCE , null , null ) , callBack );
  }

  @Override
  public void setNode( final IExpressionNode node ) {
    this.node = node;
  }

  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  @Override
  public ValueVector next() throws IOException {
    rootVector.clear();
    List<ColumnBinary> columnBinaryList = reader.nextRaw();
    int rowCount = reader.getCurrentSpreadSize();
    IMemoryAllocator memoryAllocator =
        rootMemoryAllocator.create( allocator , rootVector , rowCount );

    if ( Objects.nonNull(node) ) {
      BlockIndexNode blockIndexNode = new BlockIndexNode();
      for ( ColumnBinary columnBinary : columnBinaryList ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( blockIndexNode , columnBinary , 0 );
      }
      List<Integer> blockIndexList = node.getBlockSpreadIndex( blockIndexNode );
      if ( Objects.nonNull(blockIndexList) && blockIndexList.isEmpty() ) {
        memoryAllocator.setValueCount( 0 );
        return rootVector;
      }
    }

    int spreadSize = reader.getCurrentSpreadSize();
    memoryAllocator.setValueCount( spreadSize );
    for ( ColumnBinary columnBinary : columnBinaryList ) {
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
      IMemoryAllocator childMemoryAllocator =
          memoryAllocator.getChild( columnBinary.columnName , columnBinary.columnType );
      maker.loadInMemoryStorage( columnBinary , childMemoryAllocator );
      childMemoryAllocator.setValueCount( spreadSize );
    }
    return rootVector;
  }

  @Override
  public void close() throws IOException {
    rootVector.clear();
    reader.close();
  }

}
