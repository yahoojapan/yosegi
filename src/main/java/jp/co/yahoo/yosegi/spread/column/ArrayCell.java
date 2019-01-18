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

package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.spread.Spread;

import java.util.List;

public class ArrayCell implements ICell<SpreadArrayLink,List<ICell>> {

  private SpreadArrayLink spreadArrayLink;

  public ArrayCell( final SpreadArrayLink spreadArrayLink ) {
    this.spreadArrayLink = spreadArrayLink;
  }

  public int getParentIndex() {
    return spreadArrayLink.getParentIndex();
  }

  public int getStart() {
    return spreadArrayLink.getStart();
  }

  public int getEnd() {
    return spreadArrayLink.getEnd();
  }

  public Spread getSpread() {
    return spreadArrayLink.getSpread();
  }

  public ICell getArrayRow( final int index ) {
    return spreadArrayLink.getArrayRow( index );
  }

  @Override
  public List<ICell> getRow() {
    return spreadArrayLink.getLine();
  }

  @Override
  public void setRow( final SpreadArrayLink spreadArrayLink ) {
    this.spreadArrayLink = spreadArrayLink;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.ARRAY;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    List<ICell> line = spreadArrayLink.getLine();
    result.append( String.format( "(%s)" , getType() ) );
    result.append( line.toString() );

    return result.toString();
  }

}
