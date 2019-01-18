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

package jp.co.yahoo.yosegi.spread;

import jp.co.yahoo.yosegi.spread.column.ArrowColumnFactory;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

public final class ArrowSpreadUtil {

  /**
   * Create a Spread from the Arrow Vector list and the row size.
   */
  public static Spread toSpread( final int rowCount , final List<FieldVector> vectorList ) {
    Spread spread = new Spread();
    for ( ValueVector vector : vectorList ) {
      spread.addColumn( ArrowColumnFactory.convert( vector.getField().getName() , vector ) );
    }
    spread.setRowCount( rowCount );
    return spread;
  }

}
