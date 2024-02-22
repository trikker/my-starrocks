// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

public class OracleSchemaResolver extends JDBCSchemaResolver {

    private static HashSet<String> internalSchemaSet = new HashSet<>(Arrays.asList(
            "ANONYMOUS", "APPQOSSYS", "AUDSYS", "CTXSYS", "DBSFWUSER",
            "DBSNMP", "DIP", "DVF", "DVSYS", "GGSYS", "GSMADMIN_INTERNAL",
            "GSMCATUSER", "GSMUSER", "LBACSYS", "MDDATA", "MDSYS",
            "OJVMSYS", "OLAPSYS", "ORACLE_OCM", "ORDDATA", "ORDPLUGINS",
            "ORDSYS", "OUTLN", "REMOTE_SCHEDULER_AGENT", "SI_INFORMTN_SCHEMA",
            "SYS", "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM",
            "SYSRAC", "SYSTEM", "WMSYS", "XDB", "XS$NULL"
    ));
    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!internalSchemaSet.contains(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            // In Oracle, need to close the resultSet otherwise may casuse the following error:
            // ORA-01000: maximum open cursors exceeded
            resultSet.close();
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }
    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null,
                new String[] {"TABLE", "SYNONYM", "VIEW", "MATERIALIZED VIEW"});
    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }

    @Override
    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));
            String columnName = columnSet.getString("COLUMN_NAME");
            fullSchema.add(new Column(columnName, type,
                    columnSet.getString("IS_NULLABLE").equals("YES")));
        }
        return fullSchema;
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, String dbName, String catalogName,
                          Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, dbName, catalogName, newProp);
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, newProp);
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            // VARCHAR2(size [BYTE | CHAR])
            case Types.VARCHAR:
            // NVARCHAR2(size)
            case Types.NVARCHAR:
                // VARCHAR2(N CHAR) can hold N characters which is up to N * 3 bytes in UTF8.
                return ScalarType.createVarcharType(columnSize * 3);

            // CHAR[(size [BYTE | CHAR])]
            case Types.CHAR:
            // NCHAR[(size)]
            case Types.NCHAR:
                // CHAR(N CHAR) can hold N characters which is up to N * 3 bytes in UTF8.
                return ScalarType.createCharType(columnSize * 3);

            // RAW(size), can be up to 32767 bytes
            case Types.VARBINARY:
            // LONG RAW, can be up to 2GB bytes
            case Types.LONGVARBINARY:
            // LONG, can be up to 2GB bytes
            // WARN: length of max_varchar_length(1048576) may be NOT enough.
            case Types.LONGVARCHAR:
            // CLOB, maximum size is (4 gigabytes - 1) * (database block size).
            // WARN: length of max_varchar_length(1048576) may be NOT enough.
            case Types.CLOB:
            // NCLOB, maximum size is (4 gigabytes - 1) * (database block size).
            // WARN: length of max_varchar_length(1048576) may be NOT enough.
            case Types.NCLOB:
                return ScalarType.createOlapMaxVarcharType();

            // NUMBER[(p[,s])]
            case Types.NUMERIC:
                primitiveType = PrimitiveType.DECIMAL32;
                break;

            // FLOAT[(p)]
            case Types.FLOAT:
                primitiveType = PrimitiveType.DOUBLE;
                break;

            // DATE, TIMESTAMP [(fractional_seconds_precision)]
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;

            // BINARY_FLOAT, BINARY_DOUBLE, INTERVAL, ROWID, TIMESTAMP WITH [LOCAL] TIME ZONE, UROWID, BLOB, BFILE
            default:
                if (typeName.equals("BINARY_FLOAT") || typeName.equals("BINARY_DOUBLE")) {
                    primitiveType = PrimitiveType.DOUBLE;
                } else if (typeName.startsWith("INTERVAL") || typeName.equals("ROWID") ||
                    // TIMESTAMP[(s)] WITH TIME ZONE and TIMESTAMP[(s)] WITH LOCAL TIME ZONE
                    typeName.endsWith("TIME ZONE")) {
                    return ScalarType.createVarcharType(100);
                } else if (typeName.equals("UROWID")) {
                    return ScalarType.createVarcharType(4000);
                } else {
                    // Currently, only BLOB and BFILE will go here.
                    primitiveType = PrimitiveType.UNKNOWN_TYPE;
                }
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            // 1. In Oracle, digits can be negtive. So precision should be precision + |digits| after being converted.
            // 2. In Oracle, columnSize can be less than digits. So precision should be no less than digits.
            int precision = max(digits, columnSize + max(-digits, 0));
            // if user not specify NUMBER precision and scale, the default value is 0,
            // we can't defer the precision and scale, can only deal it as string.

            // When NUMBER has no p and s, columnSize is 0 and digits is -127.
            if (columnSize == 0) {
                return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
            }
            return ScalarType.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), TimeUtils.getEpochSeconds()));
    }

}
