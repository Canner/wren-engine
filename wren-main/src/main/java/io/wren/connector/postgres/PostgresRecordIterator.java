/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.wren.connector.postgres;

import io.wren.base.Parameter;
import io.wren.base.client.Client;
import io.wren.base.client.jdbc.BaseJdbcRecordIterator;
import org.joda.time.Period;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class PostgresRecordIterator
        extends BaseJdbcRecordIterator<Object[]>
{
    public static PostgresRecordIterator of(Client client, String sql)

            throws SQLException
    {
        return of(client, sql, emptyList());
    }

    public static PostgresRecordIterator of(Client client, String sql, List<Parameter> parameters)
            throws SQLException
    {
        return new PostgresRecordIterator(client, sql, parameters);
    }

    private PostgresRecordIterator(Client client, String sql, List<Parameter> parameters)
            throws SQLException
    {
        super(client, sql, parameters);
    }

    @Override
    public Object[] getCurrentRecord()
            throws SQLException
    {
        List<Object> builder = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            if (resultSet.getMetaData().getColumnType(i) == Types.BLOB) {
                Blob blob = resultSet.getBlob(i);
                byte[] bytes = blob.getBytes(0, (int) blob.length());
                builder.add(bytes);
            }
            else if (resultSet.getMetaData().getColumnType(i) == Types.SMALLINT) {
                builder.add(resultSet.getShort(i));
            }
            else if (resultSet.getMetaData().getColumnType(i) == Types.TIMESTAMP) {
                builder.add(resultSet.getTimestamp(i).toLocalDateTime());
            }
            else if (resultSet.getMetaData().getColumnType(i) == Types.ARRAY) {
                List<Object> objArray = Optional.ofNullable(resultSet.getArray(i))
                        .map(array -> {
                            try {
                                return Arrays.stream((Object[]) array.getArray()).map(obj -> {
                                    if (obj instanceof PGobject) {
                                        return getPgObjectValue((PGobject) obj);
                                    }
                                    if (obj instanceof Timestamp) {
                                        return ((Timestamp) obj).toLocalDateTime();
                                    }
                                    if (obj instanceof Date) {
                                        return ((Date) obj).toLocalDate();
                                    }
                                    return obj;
                                }).collect(Collectors.toList());
                            }
                            catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }).orElse(null);
                builder.add(objArray);
            }
            else if (resultSet.getMetaData().getColumnType(i) == Types.DATE) {
                builder.add(resultSet.getDate(i).toLocalDate());
            }
            else {
                Object obj = resultSet.getObject(i);
                if (obj instanceof PGInterval) {
                    PGInterval pgInterval = (PGInterval) obj;
                    builder.add(new Period(
                            pgInterval.getYears(),
                            pgInterval.getMonths(),
                            0,
                            pgInterval.getDays(),
                            pgInterval.getHours(),
                            pgInterval.getMinutes(),
                            pgInterval.getWholeSeconds(),
                            pgInterval.getMicroSeconds() / 1000));
                }
                else if (obj instanceof PGobject) {
                    builder.add(getPgObjectValue((PGobject) obj));
                }
                else {
                    builder.add(obj);
                }
            }
        }
        return builder.toArray();
    }

    public Object getPgObjectValue(PGobject pgObject)
    {
        if (pgObject == null) {
            return null;
        }

        if (pgObject.getType().equals("xid")) {
            return Optional.ofNullable(pgObject.getValue()).map(Integer::parseInt).orElse(null);
        }

        return pgObject.getValue();
    }

    @Override
    protected void setParameter(List<Parameter> parameters)
            throws SQLException
    {
        PostgresClient.setParameter(statement, parameters);
    }
}
