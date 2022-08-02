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

import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.NullColumn;

import java.io.IOException;

public class YosegiNullLoader implements ILoader<IColumn> {

  private final int loadSize;

  public YosegiNullLoader( final int loadSize ) {
    this.loadSize = loadSize;
  }

  @Override
  public LoadType getLoaderType() {
    return LoadType.NULL;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull( final int index ) throws IOException {}

  @Override
  public void finish() throws IOException {}

  @Override
  public IColumn build() throws IOException {
    return NullColumn.getInstance();
  }

  @Override
  public boolean isLoadingSkipped() {
    return true;
  }

}
