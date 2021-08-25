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
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;

public class YosegiLoaderFactory implements ILoaderFactory {

  @Override
  public ILoader<IColumn> createLoader(
      final ColumnBinary columnBinary ,
      final int loadSize ,
      final LoadType loadType ) throws IOException {
    switch ( loadType ) {
      case NULL :
        return new YosegiNullLoader( loadSize );
      case SEQUENTIAL :
        return new YosegiSequentialLoader( columnBinary , loadSize );
      case DICTIONARY :
        return new YosegiDictionaryLoader( columnBinary , loadSize );
      default: throw new IOException( "This type is not supported : " + loadType );
    }
  }

}
