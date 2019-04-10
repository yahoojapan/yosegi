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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.util.FindClass;

import java.io.IOException;
import java.util.Objects;

public final class FindBlockWriter {

  private FindBlockWriter() {}

  /**
   * Get IBlockWriter from class name.
   */
  public static IBlockWriter get(final String target) throws IOException {
    if (Objects.isNull(target) || target.isEmpty()) {
      throw new IOException("IBlockWriter class name is null or empty.");
    }
    Object obj = FindClass.getObject(target, true, FindBlockWriter.class.getClassLoader());
    if (!IBlockWriter.class.isInstance(obj)) {
      throw new IOException("Invalid IBlockWriter class : " + target);
    }
    return (IBlockWriter)obj;
  }
}

