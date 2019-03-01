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

import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.CompressionPolicy;

import java.util.HashMap;
import java.util.Map;

public class CompressResultNode {

  private final Map<String,Map<String,CompressResult>> currentCompressResult;
  private final Map<String,CompressResultNode> childNode;

  public CompressResultNode() {
    currentCompressResult = new HashMap<String,Map<String,CompressResult>>();
    childNode = new HashMap<String,CompressResultNode>();
  }

  /**
   * Get child node.
   */
  public CompressResultNode getChild( final String childName ) {
    if ( ! childNode.containsKey( childName ) ) {
      childNode.put( childName , new CompressResultNode() );
    }
    return childNode.get( childName );
  }

  /**
   * Get ColressBinaryMaker class name and CompressResult from the name that identifies the process.
   */
  public CompressResult getCompressResult(
      final String makerClassName ,
      final String processName ,
      final CompressionPolicy compressionPolicy ,
      final double allowedRatio ) {
    if ( ! currentCompressResult.containsKey( makerClassName ) ) {
      currentCompressResult.put( makerClassName , new HashMap<String,CompressResult>() );
    }
    Map<String,CompressResult> makerCompressResultMap
        = currentCompressResult.get( makerClassName );
    if ( ! makerCompressResultMap.containsKey( processName ) ) {
      makerCompressResultMap.put(
          processName , new CompressResult( compressionPolicy , allowedRatio ) );
    }
    return makerCompressResultMap.get( processName );
  }

}
