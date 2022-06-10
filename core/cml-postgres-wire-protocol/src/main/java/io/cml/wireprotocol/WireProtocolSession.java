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

package io.cml.wireprotocol;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.cml.pgcatalog.regtype.RegObjectFactory;
import io.cml.spi.CmlException;
import io.cml.spi.Column;
import io.cml.spi.ConnectorRecordIterable;
import io.cml.spi.connector.Connector;
import io.cml.sql.PostgreSqlRewrite;
import io.cml.sql.SqlConverter;
import io.cml.wireprotocol.patterns.PostgreSqlRewriteUtil;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Statement;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.spi.metadata.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static io.cml.wireprotocol.PostgresWireProtocol.isIgnoredCommand;
import static io.cml.wireprotocol.PostgresWireProtocolErrorCode.INVALID_PREPARED_STATEMENT_NAME;
import static io.trino.execution.ParameterExtractor.getParameterCount;
import static io.trino.execution.sql.SqlFormatterUtil.getFormattedSql;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class WireProtocolSession
{
    private static final Logger LOG = Logger.get(WireProtocolSession.class);

    private static final List<Class<?>> SESSION_COMMAND = ImmutableList.of(Deallocate.class);

    private static final ParsingOptions PARSE_AS_DECIMAL = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private static final String ALL = "all";

    private Properties properties;
    private final PreparedStatementMap preparedStatements = new PreparedStatementMap();
    private final PortalMap portals = new PortalMap();
    private final List<String> sessionProperties = new ArrayList<>();
    private CompletableFuture<Optional<GenericTableRecordIterable>> runningQuery = CompletableFuture.completedFuture(null);
    private final PostgreSqlRewrite postgreSqlRewrite;
    private final SqlParser sqlParser;
    private final RegObjectFactory regObjectFactory;
    private final Connector connector;

    private final SqlConverter sqlConverter;

    public WireProtocolSession(RegObjectFactory regObjectFactory, Connector connector, SqlConverter sqlConverter)
    {
        this.postgreSqlRewrite = new PostgreSqlRewrite();
        this.sqlParser = new SqlParser();
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.sqlConverter = sqlConverter;
    }

    public int getParamTypeOid(String statementName, int fieldPosition)
    {
        if (preparedStatements.containsKey(statementName)) {
            return preparedStatements.get(statementName).getParamTypeOids().get(fieldPosition);
        }
        else {
            throw new CmlException(NOT_FOUND, format("prepared statement %s not found", statementName));
        }
    }

    public Portal getPortal(String portalName)
    {
        if (portals.containsKey(portalName)) {
            return portals.get(portalName);
        }
        else {
            throw new CmlException(NOT_FOUND, format("portal %s not found", portalName));
        }
    }

    @Nullable
    public String getOriginalStatement(String statementName)
    {
        if (preparedStatements.containsKey(statementName)) {
            return preparedStatements.get(statementName).getOriginalStatement();
        }
        return null;
    }

    @Nullable
    public FormatCodes.FormatCode[] getResultFormatCodes(String portal)
    {
        return getPortal(portal).getResultFormatCodes();
    }

    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(properties.getProperty("password"));
    }

    @Nullable
    public String getClientUser()
    {
        return properties.getProperty("user");
    }

    public String getCurrentWorkspace()
    {
        return properties.getProperty("database");
    }

    public List<String> getTrinoSessionProperties()
    {
        return sessionProperties;
    }

    public boolean doAuthentication(String password)
    {
        return true;
    }

    public Optional<List<Column>> describePortal(String name)
    {
        throw new UnsupportedOperationException();
    }

    public List<Integer> describeStatement(String name)
    {
        throw new UnsupportedOperationException();
    }

    public void parse(String statementName, String statement, List<Integer> paramTypes)
    {
        if (statementName.equalsIgnoreCase(ALL)) {
            throw new CmlException(INVALID_PREPARED_STATEMENT_NAME, format("%s is a preserved word. Can't be the name of prepared statement", statementName));
        }
        String statementTrimmed = rewritePreparedChar(statement.split(";")[0].trim());
        if (statementTrimmed.isEmpty() || isIgnoredCommand(statementTrimmed)) {
            preparedStatements.put(statementName, new PreparedStatement(statementName, "", paramTypes, statementTrimmed, false));
        }
        else {
            String statementPreRewritten = PostgreSqlRewriteUtil.rewrite(statementTrimmed);
            // validateSetSessionProperty(statementPreRewritten);
            Statement parsedStatement = sqlParser.createStatement(statementPreRewritten, PARSE_AS_DECIMAL);
            Statement rewrittenStatement = postgreSqlRewrite.rewrite(regObjectFactory, parsedStatement);
            List<Integer> rewrittenParamTypes = rewriteParameters(rewrittenStatement, paramTypes);
            preparedStatements.put(statementName,
                    new PreparedStatement(statementName, getFormattedSql(rewrittenStatement, sqlParser), rewrittenParamTypes, statementTrimmed, isSessionCommand(rewrittenStatement)));
            LOG.info("Create preparedStatement %s", statementName);
        }
    }

    private static boolean isSessionCommand(Statement statement)
    {
        return SESSION_COMMAND.contains(statement.getClass());
    }

    /**
     * JDBC will transfer the prepared parameter sign `?` to `$[0-9]+`.
     * e.g SELECT ? as c1, ? as c2  -> SELECT $1 as c1, $2 c2
     * We need to transfer it back to `?` to match the syntax of presto.
     */
    private String rewritePreparedChar(String statement)
    {
        return statement.replaceAll("\\$[0-9]+", "?");
    }

    public void bind(String portalName, String statementName, List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes)
    {
        portals.put(portalName, new Portal(preparedStatements.get(statementName), params, resultFormatCodes));
        String paramString = params.stream()
                .map(element -> (isNull(element)) ? "null" : element.toString())
                .collect(Collectors.joining(","));
        LOG.info("Bind Portal %s with parameters %s to Statement %s", portalName, paramString, statementName);
    }

    public CompletableFuture<Optional<ConnectorRecordIterable>> execute(String portalName)
    {
        return execute(portals.get(portalName));
    }

    private CompletableFuture<Optional<ConnectorRecordIterable>> execute(Portal portal)
    {
        String execStmt = portal.getPreparedStatement().getStatement();
        return CompletableFuture.supplyAsync(() -> {
            String sql = sqlConverter.convert(execStmt);
            return Optional.of(connector.directQuery(sql));
        });
    }

    private CompletableFuture<Optional<Iterable<?>>> executeSessionCommand(Portal portal)
    {
        throw new UnsupportedOperationException();
    }

    private void closePortalIfDeallocated(Map.Entry<String, Portal> entry, String deallocatedName)
    {
        Portal portal = entry.getValue();
        if (portal.getPreparedStatement().getName().equals(deallocatedName)) {
            portals.remove(entry.getKey());
        }
    }

    public CompletableFuture<Optional<GenericTableRecordIterable>> sync()
    {
        CompletableFuture<Optional<GenericTableRecordIterable>> ended = runningQuery;
        runningQuery = CompletableFuture.completedFuture(null);
        return ended;
    }

    public void close(byte type, String name)
    {
        switch (type) {
            case 'P':
                portals.remove(name);
                break;
            case 'S':
                PreparedStatement preparedStatement = preparedStatements.remove(name);
                if (preparedStatement != null) {
                    List<String> removedNames = portals.entrySet().stream()
                            .filter(entry -> entry.getValue().getPreparedStatement().getName().equals(preparedStatement.getName()))
                            .map(Map.Entry::getKey).collect(toImmutableList());
                    removedNames.forEach(portals::remove);
                }
                break;
            default:
                throw new CmlException(INVALID_PARAMETER_USAGE, format("Type %s is invalid. We only support 'P' and 'S'", type));
        }
    }

    private static class PreparedStatementMap
    {
        private final HashMap<String, PreparedStatement> delegate = new HashMap<>();

        public PreparedStatement get(String key)
        {
            if (key.isEmpty()) {
                return delegate.get(PreparedStatement.CANNERFLOW_RESERVED_PREPARE_NAME);
            }
            return delegate.get(key);
        }

        public PreparedStatement put(String key, PreparedStatement value)
        {
            if (key.isEmpty()) {
                return delegate.put(PreparedStatement.CANNERFLOW_RESERVED_PREPARE_NAME, value);
            }
            return delegate.put(key, value);
        }

        public PreparedStatement remove(String key)
        {
            if (key.isEmpty()) {
                return delegate.remove(PreparedStatement.CANNERFLOW_RESERVED_PREPARE_NAME);
            }
            return delegate.remove(key);
        }

        public boolean containsKey(String key)
        {
            if (key.isEmpty()) {
                return delegate.containsKey(PreparedStatement.CANNERFLOW_RESERVED_PREPARE_NAME);
            }
            return delegate.containsKey(key);
        }

        public Collection<PreparedStatement> values()
        {
            return delegate.values();
        }

        public void clear()
        {
            delegate.clear();
        }
    }

    private static class PortalMap
    {
        private final Map<String, Portal> delegate = new ConcurrentHashMap<>();

        public Portal get(String key)
        {
            return delegate.get(key);
        }

        public Portal put(String key, Portal value)
        {
            close(key);
            return delegate.put(key, value);
        }

        public Portal remove(String key)
        {
            close(key);
            return delegate.remove(key);
        }

        public boolean containsKey(String key)
        {
            return delegate.containsKey(key);
        }

        public Set<Map.Entry<String, Portal>> entrySet()
        {
            return delegate.entrySet();
        }

        private void close(String key)
        {
            if (delegate.get(key) != null) {
                delegate.get(key).close();
            }
        }
    }

    private String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    private boolean isNoDataReturnedCommand(String statement)
    {
        return statement.toUpperCase(ENGLISH).startsWith("SET");
    }

    private List<Integer> rewriteParameters(Statement statement, List<Integer> paramTypes)
    {
        int parameters = getParameterCount(statement);

        if (paramTypes.size() >= parameters) {
            return paramTypes;
        }

        List<Integer> resultParamTypes = new ArrayList<>(paramTypes);
        for (int i = paramTypes.size(); i < parameters; i++) {
            resultParamTypes.add(0);
        }
        return resultParamTypes;
    }
}
