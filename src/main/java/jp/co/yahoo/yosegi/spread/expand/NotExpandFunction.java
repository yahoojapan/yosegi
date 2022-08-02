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

package jp.co.yahoo.yosegi.spread.expand;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NotExpandFunction implements IExpandFunction {

  @Override
  public int expandFromColumnBinary(
      final List<ColumnBinary> columnBinaryList , final int spreadSize ) throws IOException {
    return spreadSize;
  }

  @Override
  public void expandIndexNode( final BlockIndexNode rootNode ) throws IOException {}

  @Override
  public String[] getExpandLinkColumnName( final String linkName ) {
    return new String[0];
  }

  @Override
  public List<String[]> getExpandColumnName() {
    return new ArrayList<String[]>();
  }
}
