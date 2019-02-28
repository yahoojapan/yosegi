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

package jp.co.yahoo.yosegi.compressor;

import java.util.Map;

public class CompressResult {

  private final CompressionPolicy compressionPolicy;
  private final double allowedRatio;

  private boolean isEnd = false;
  private double startLevelDataRatio = -1.0d;
  private int currentLevel = 0;

  public CompressResult( final CompressionPolicy compressionPolicy , final double allowedRatio ) {
    this.compressionPolicy = compressionPolicy;
    this.allowedRatio = allowedRatio;
  }

  /**
   * Get CompressionPolicy.
   */
  public CompressionPolicy getCompressionPolicy() {
    return compressionPolicy;
  }

  /**
   * Get compression level.
   */
  public int getCurrentLevel() {
    return currentLevel;
  }

  /**
   * Feed back the result after compression.
   */
  public void feedBack( final int originalDataSize , final int dataSize ) {
    if ( originalDataSize <= 0 || dataSize <= 0 ) {
      return;
    }
    if ( isEnd ) {
      return;
    }
    if ( Double.valueOf( startLevelDataRatio ).equals( -1.0d ) ) {
      startLevelDataRatio = (double)dataSize / (double)originalDataSize;
    }
    double currentRatio = (double)dataSize / (double)originalDataSize;
    double ratio = currentRatio / startLevelDataRatio;
    if ( Double.valueOf( allowedRatio ).compareTo( ratio ) <= 0 ) {
      setEnd();
    } else {
      currentLevel++;
    }
  }

  /**
   * The processing for lowering the compression level ends.
   */
  public void setEnd() {
    if ( 0 < currentLevel ) {
      currentLevel--;
    }
    isEnd = true;
  }

}
