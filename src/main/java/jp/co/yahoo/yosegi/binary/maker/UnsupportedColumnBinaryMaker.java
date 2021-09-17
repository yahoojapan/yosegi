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

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.NullColumn;

import java.io.IOException;

public class UnsupportedColumnBinaryMaker implements IColumnBinaryMaker {

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    return new ColumnBinary(
        this.getClass().getName() ,
        commonConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        0 ,
        0 ,
        -1 ,
        new byte[0] ,
        0 ,
        0 ,
        null );
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary , final int loadSize ) {
    return LoadType.NULL;
  }

  @Override
  public void load(
      final ColumnBinary columnBinary ,
      final ILoader loader ) throws IOException {
    for ( int i = 0 ; i < loader.getLoadSize() ; i++ ) {
      loader.setNull(i);
    }
    loader.finish();
    return;
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return 0;
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {}

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

}
