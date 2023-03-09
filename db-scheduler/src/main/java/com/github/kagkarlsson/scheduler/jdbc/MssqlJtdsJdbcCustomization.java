/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MssqlJtdsJdbcCustomization extends MssqlJdbcCustomization {

    @Override
    public String getName() {
        return "MSSQL-jTDS";
    }

    @Override
    public byte[] getBytes(ResultSet rs, String columnName) throws SQLException {
        byte[] result;
        try (Reader reader = ((Clob) rs.getObject(columnName)).getCharacterStream()) {
            char[] charArray = new char[8 * 1024];
            StringBuilder builder = new StringBuilder();
            int numCharsRead;
            while ((numCharsRead = reader.read(charArray, 0, charArray.length)) != -1) {
                builder.append(charArray, 0, numCharsRead);
            }
            result = builder.toString().getBytes(StandardCharsets.UTF_16LE);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return result;
    }
}
