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

package jp.co.yahoo.yosegi.util;

public class DetermineMinMax<T extends Comparable> {

  private T min;
  private T max;

  public DetermineMinMax( final T min , final T max ) {
    this.min = min;
    this.max = max;
  }

  /**
   * Compare Min, Max set.
   */
  public void set( final T target ) {
    if ( min == null || 0 < min.compareTo( target ) ) {
      min = target;
    }
    if ( max == null || max.compareTo( target ) < 0 ) {
      max = target;
    }
  }

  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }

}
