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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;

public class TestNotExpandFunction {
    private NotExpandFunction target = new NotExpandFunction();
    Spread spread = new Spread();

    @BeforeEach
    public void setup() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("stringKey", new StringObj("val0"));
        dataContainer.put("integerKey", new IntegerObj(5));
        spread.addRow(dataContainer);
    }

    @Test
    public void T_expand() throws IOException {
        assertEquals(target.expand(spread).getColumnSize(), 2);
        assertEquals(target.expand(spread).getColumn("stringKey").get(0).toString(), "(STRING)val0");
        assertEquals(target.expand(spread).getColumn(1).get(0).toString(), "(INTEGER)5");
    }

    @Test
    public void T_getExpandColumnName() {
        assertEquals( target.getExpandColumnName().size() , 0 );
    }

}
