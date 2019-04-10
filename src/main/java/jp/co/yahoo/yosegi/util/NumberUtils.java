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

public final class NumberUtils {
  @FunctionalInterface
  private interface ComparationFunc {
    public boolean get();
  }

  private NumberUtils() {}

  /**
   * Check if the specified value is in the range of min, max.
   */
  public static boolean range(
      final long min ,
      final boolean minHasEquals ,
      final long max ,
      final boolean maxHasEquals ,
      final long target ) {
    ComparationFunc isMin = () -> minHasEquals ? min <= target : min < target;
    ComparationFunc isMax = () -> maxHasEquals ? target <= max : target < max;
    return isMin.get() && isMax.get();
  }

  /**
   * Check if the specified value is in the range of min, max.
   */
  public static boolean range(
      final int min ,
      final boolean minHasEquals ,
      final int max ,
      final boolean maxHasEquals ,
      final int target ) {
    ComparationFunc isMin = () -> minHasEquals ? min <= target : min < target;
    ComparationFunc isMax = () -> maxHasEquals ? target <= max : target < max;
    return isMin.get() && isMax.get();
  }

  /**
   * Check if the specified value is in the range of min, max.
   */
  public static boolean range(
      final short min ,
      final boolean minHasEquals ,
      final short max ,
      final boolean maxHasEquals ,
      final short target ) {
    ComparationFunc isMin = () -> minHasEquals ? min <= target : min < target;
    ComparationFunc isMax = () -> maxHasEquals ? target <= max : target < max;
    return isMin.get() && isMax.get();
  }

  /**
   * Check if the specified value is in the range of min, max.
   */
  public static boolean range(
      final byte min ,
      final boolean minHasEquals ,
      final byte max ,
      final boolean maxHasEquals ,
      final byte target ) {
    ComparationFunc isMin = () -> minHasEquals ? min <= target : min < target;
    ComparationFunc isMax = () -> maxHasEquals ? target <= max : target < max;
    return isMin.get() && isMax.get();
  }

  /**
   * Check if the specified value is in the range of min, max.
   */
  public static boolean range(
      final Float min ,
      final boolean minHasEquals ,
      final Float max ,
      final boolean maxHasEquals ,
      final Float target ) {

    ComparationFunc isMin = () -> {
      int res = min.compareTo(target);
      return minHasEquals ? res <= 0 : res < 0;
    };
    ComparationFunc isMax = () -> {
      int res = max.compareTo(target);
      return maxHasEquals ? 0 <= res : 0 < res;
    };
    return isMin.get() && isMax.get();
  }
 
  /**
   * Check if the specified value is in the range of min, max.
   */
  public static boolean range(
      final Double min ,
      final boolean minHasEquals ,
      final Double max ,
      final boolean maxHasEquals ,
      final Double target ) {

    ComparationFunc isMin = () -> {
      int res = min.compareTo(target);
      return minHasEquals ? res <= 0 : res < 0;
    };
    ComparationFunc isMax = () -> {
      int res = max.compareTo(target);
      return maxHasEquals ? 0 <= res : 0 < res;
    };
    return isMin.get() && isMax.get();
  }
}

