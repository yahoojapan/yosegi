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

package jp.co.yahoo.yosegi.binary.optimizer;

import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.StringColumnAnalizeResult;

import java.io.IOException;

public class StringOptimizer implements IOptimizer {

  private final IColumnBinaryMaker[] makerArray;

  /**
   * Select logic to convert String.
   */
  public StringOptimizer( final Configuration config ) throws IOException {
    makerArray = new IColumnBinaryMaker[]{
      FindColumnBinaryMaker.get(
          "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeStringColumnBinaryMaker" ),
      FindColumnBinaryMaker.get(
          "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker" ),
    };
  }

  @Override
  public ColumnBinaryMakerConfig getColumnBinaryMakerConfig(
        final ColumnBinaryMakerConfig commonConfig ,
        final IColumnAnalizeResult analizeResult ) {
    IColumnBinaryMaker maker = null;
    int minSize = Integer.MAX_VALUE;

    StringColumnAnalizeResult stringResult = (StringColumnAnalizeResult)analizeResult;

    int avgLength = stringResult.getTotalUtf8ByteSize() / stringResult.getRowCount();
    if ( 4 < avgLength
        && ( (double)stringResult.getUniqCount() / (double)stringResult.getRowCount() ) < 0.5d ) {
      maker = makerArray[0];
    } else {
      for ( IColumnBinaryMaker currentMaker : makerArray ) {
        int currentSize = currentMaker.calcBinarySize( analizeResult );
        if ( currentSize <= minSize ) {
          maker = currentMaker;
          minSize = currentSize;
        }
      }
    }
    ColumnBinaryMakerConfig currentConfig = null;
    if ( maker != null ) {
      currentConfig = new ColumnBinaryMakerConfig( commonConfig );
      currentConfig.stringMakerClass = maker;
    }
    return currentConfig;
  }

}
