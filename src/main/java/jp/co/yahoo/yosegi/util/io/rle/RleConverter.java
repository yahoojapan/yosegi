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

package jp.co.yahoo.yosegi.util.io.rle;

import java.io.IOException;

public class RleConverter<T> {

  private boolean isFinish = false;

  private final T[] valueArray;
  private final int[] lengthArray;
  private int rowGroupCount;
  private int maxRowGroupLength;
  private T currentValue;
  private int currentLength;

  /**
   * Initialization by specifying initial value and buffer size.
   */
  public RleConverter( final T firstValue , final T[] valueArray ) {
    this.valueArray = valueArray;
    if ( valueArray == null ) {
      lengthArray = null;
    } else {
      lengthArray = new int[valueArray.length];
    }
    currentValue = firstValue;
  }

  /**
   * Add value.
   */
  public void add( final T value ) throws IOException {
    if ( isFinish ) {
      throw new IOException( "Processing has already been completed." );
    }
    if ( ! currentValue.equals( value ) ) {
      if ( valueArray != null ) {
        valueArray[rowGroupCount] = currentValue;
        lengthArray[rowGroupCount] = currentLength;
      }
      rowGroupCount++;
      if ( maxRowGroupLength < currentLength ) {
        maxRowGroupLength = currentLength;
      }
      currentValue = value;
      currentLength = 0;
    }
    currentLength++;
  }

  /**
   * Finish adding values.
   */
  public void finish() throws IOException {
    if ( isFinish ) {
      throw new IOException( "Processing has already been completed." );
    }
    if ( currentLength != 0 ) {
      if ( valueArray != null ) {
        valueArray[rowGroupCount] = currentValue;
        lengthArray[rowGroupCount] = currentLength;
      }
      rowGroupCount++;
      if ( maxRowGroupLength < currentLength ) {
        maxRowGroupLength = currentLength;
      }
    }
    isFinish = true;
  }

  /**
   * Get array of value.
   */
  public T[] getValueArray() throws IOException {
    if ( ! isFinish ) {
      throw new IOException( "Finish is not running." );
    }
    return valueArray;
  }

  /**
   * Get array of length.
   */
  public int[] getLengthArray() throws IOException {
    if ( ! isFinish ) {
      throw new IOException( "Finish is not running." );
    }
    return lengthArray;
  }

  /**
   * Get row goup count.
   */
  public int getRowGroupCount() throws IOException {
    if ( ! isFinish ) {
      throw new IOException( "Finish is not running." );
    }
    return rowGroupCount;
  }

  /**
   * Get maximum length of group.
   */
  public int getMaxGroupLength() throws IOException {
    if ( ! isFinish ) {
      throw new IOException( "Finish is not running." );
    }
    return maxRowGroupLength;
  }

}
