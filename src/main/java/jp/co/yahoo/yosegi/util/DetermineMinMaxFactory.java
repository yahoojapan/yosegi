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

public final class DetermineMinMaxFactory {

  private DetermineMinMaxFactory() {}

  public static DetermineMinMax<Integer> createInt() {
    return new DetermineMinMax<Integer>( Integer.MAX_VALUE , Integer.MIN_VALUE );
  }

  public static DetermineMinMax<Long> createLong() {
    return new DetermineMinMax<Long>( Long.MAX_VALUE , Long.MIN_VALUE );
  }

  public static DetermineMinMax<Double> createDouble() {
    return new DetermineMinMax<Double>( Double.MAX_VALUE , -Double.MAX_VALUE );
  }

  public static DetermineMinMax<Float> createFloat() {
    return new DetermineMinMax<Float>( Float.MAX_VALUE , -Float.MAX_VALUE );
  }

  public static DetermineMinMax<String> createString() {
    return new DetermineMinMax<String>( null , "" );
  }

}
