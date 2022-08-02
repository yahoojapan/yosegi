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

import jp.co.yahoo.yosegi.binary.ColumnBinary;

import java.io.IOException;

public interface IRunLengthEncodingArrayLoader<T> extends ILoader<T> {

  @Override
  default LoadType getLoaderType() {
    return LoadType.RLE_ARRAY;
  }

  @Override
  default void setNull( final int index ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setNull()" );
  }

  void setRowGroupCount( final int count ) throws IOException;

  void setNullAndRepetitions(
      final int startIndex ,
      final int repetitions ,
      final int rowGroupIndex )  throws IOException;

  void setRowGourpIndexAndRepetitions(
      final int startIndex ,
      final int repetitions ,
      final int rowGroupIndex ,
      final int rowGroupStart ,
      final int rowGourpLength ) throws IOException;

  void loadChild(
      final ColumnBinary columnBinary , final int childLength ) throws IOException;

}
