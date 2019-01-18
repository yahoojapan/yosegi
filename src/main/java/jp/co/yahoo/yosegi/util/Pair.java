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

import java.util.HashMap;
import java.util.Map;

public class Pair {

  private final Map<String,String> m1 = new HashMap<String,String>();
  private final Map<String,String> m2 = new HashMap<String,String>();

  public void set( final String p1 , final String p2 ) {
    m1.put( p1 , p2 );
    m2.put( p2 , p1 );
  }

  public String getPair1( final String p2 ) {
    return m2.get( p2 );
  }

  public String getPair2( final String p1 ) {
    return m1.get( p1 );
  }

}
