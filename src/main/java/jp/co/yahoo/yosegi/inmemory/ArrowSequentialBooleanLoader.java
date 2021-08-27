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

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;

public class ArrowSequentialBooleanLoader implements ISequentialLoader<ValueVector> {

  private final BitVector vector;
  private final int loadSize;

  /**
   * A loader that holds elements sequentially.
   */
  public ArrowSequentialBooleanLoader( final ValueVector vector , final int loadSize ) {
    this.vector = (BitVector)vector;
    vector.allocateNew();
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public ValueVector build() throws IOException {
    return vector;
  }

  @Override
  public void finish() throws IOException {
  }

  @Override
  public void setNull( final int index ) throws IOException {
    vector.setNull( index );
  }

  @Override
  public void setBoolean( final int index , final boolean value ) throws IOException {
    if ( value ) {
      vector.setSafe( index , 1 );
    } else {
      vector.setSafe( index , 0 );
    }
  }

}
