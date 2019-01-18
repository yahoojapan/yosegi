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

package jp.co.yahoo.yosegi.binary;

import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.util.FindClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class FindColumnBinaryMaker {

  private static final Object LOCK = new Object();
  private static final Map<String,IColumnBinaryMaker> CACHE =
      new HashMap<String,IColumnBinaryMaker>();

  private FindColumnBinaryMaker() {}

  /**
   * Create an IColumnBinaryMaker from the class name.
   */
  public static IColumnBinaryMaker get( final String target ) throws IOException {
    if ( CACHE.containsKey( target ) ) {
      return CACHE.get( target );
    }
    if ( target == null || target.isEmpty() ) {
      throw new IOException( "IColumnBinaryMaker class name is null or empty." );
    }
    Object obj = FindClass.getObject(
        target , true , FindColumnBinaryMaker.class.getClassLoader() );
    if ( ! ( obj instanceof IColumnBinaryMaker ) ) {
      throw new IOException( "Invalid IColumnBinaryMaker class : " + target );
    }
    if ( ! CACHE.containsKey( target ) ) {
      synchronized ( LOCK ) {
        if ( ! CACHE.containsKey( target ) ) {
          CACHE.put( target , (IColumnBinaryMaker)obj );
        }
      }
    }
    return (IColumnBinaryMaker)obj;
  }

}
