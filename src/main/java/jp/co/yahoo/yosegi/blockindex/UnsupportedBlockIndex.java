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

package jp.co.yahoo.yosegi.blockindex;

import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

import java.util.List;

public class UnsupportedBlockIndex implements IBlockIndex {

  public static final IBlockIndex INSTANCE = new UnsupportedBlockIndex();

  @Override
  public IBlockIndex clone() {
    return INSTANCE;
  }

  @Override
  public BlockIndexType getBlockIndexType() {
    return BlockIndexType.UNSUPPORTED;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ) {
    return false;
  }

  @Override
  public int getBinarySize() {
    return 0;
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ) {
  }

  @Override
  public List<Integer> getBlockSpreadIndex( final IFilter filter ) {
    return null;
  }

  @Override
  public IBlockIndex getNewInstance() {
    return INSTANCE;
  }

}
