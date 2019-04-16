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

package jp.co.yahoo.yosegi.blockindex;

import jp.co.yahoo.yosegi.util.FindClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class FindBlockIndex {

  private static final Map<String,IBlockIndex> CACHE = new HashMap<String,IBlockIndex>();

  private FindBlockIndex() {}

  /**
   * Get IBlockIndex from class name.
   */
  public static IBlockIndex get( final String target ) throws IOException {
    IBlockIndex cacheResult = CACHE.get( target );
    if ( Objects.nonNull(cacheResult) ) {
      return cacheResult.getNewInstance();
    }

    if ( Objects.isNull(target) || target.isEmpty() ) {
      throw new IOException( "IBlockIndex class name is null or empty." );
    }
    Object obj = FindClass.getObject( target , true , FindBlockIndex.class.getClassLoader() );
    if ( !IBlockIndex.class.isInstance(obj) ) {
      throw new IOException( "Invalid IBlockIndex class : " + target );
    }
    CACHE.put( target , (IBlockIndex)obj );
    return ( (IBlockIndex)obj ).getNewInstance();
  }
}

