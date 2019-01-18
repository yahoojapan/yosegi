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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestArrayColumn {

    @Test
    public void T_constructNotException() {
        new ArrayColumn("column_name");
    }

    @Test
    public void T_setSpreadNotException() {
        ArrayColumn target = new ArrayColumn("column_name");
        Spread spread = new Spread();
        target.setSpread(spread);
    }

    @Test
    public void T_getColumnName() {
        ArrayColumn target = new ArrayColumn("column_name");
        assertEquals(target.getColumnName(), "column_name");
    }

    @Test
    public void T_getColumnType() {
        ArrayColumn target = new ArrayColumn("column_name");
        assertEquals(target.getColumnType(), ColumnType.ARRAY);
    }

    @Test
    public void T_set_get_ParentsColumn() throws IOException {
        Spread spread = new Spread();
        Map<String,Object> dataContainer = new LinkedHashMap<String,Object>();
        dataContainer.put("parents_column_key", new StringObj("val"));
        spread.addRow( dataContainer );
        IColumn icolumn = spread.getColumn("parents_column_key");

        ArrayColumn target = new ArrayColumn("column_name");
        target.setParentsColumn(icolumn);
        assertEquals(target.getParentsColumn().getColumnName(), "parents_column_key");
    }

}
