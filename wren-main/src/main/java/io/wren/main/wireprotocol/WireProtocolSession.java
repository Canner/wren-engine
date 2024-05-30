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

package io.wren.main.wireprotocol;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.SessionContext;
import io.wren.base.WrenException;
import io.wren.base.config.WrenConfig;
import io.wren.base.sql.SqlConverter;
import io.wren.base.sqlrewrite.CacheRewrite;
import io.wren.base.sqlrewrite.WrenPlanner;
import io.wren.base.wireprotocol.PgMetastore;
import io.wren.cache.CacheManager;
import io.wren.cache.CachedTableMapping;
import io.wren.main.WrenMetastore;
import io.wren.main.metadata.Metadata;
import io.wren.main.pgcatalog.regtype.RegObjectFactory;
import io.wren.main.sql.PostgreSqlRewrite;
import io.wren.main.wireprotocol.auth.Authentication;
import io.wren.main.wireprotocol.message.Close;
import io.wren.main.wireprotocol.patterns.PostgreSqlRewriteUtil;

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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.ParameterExtractor.getParameterCount;
import static io.trino.execution.sql.SqlFormatterUtil.getFormattedSql;
import static io.wren.base.metadata.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.wren.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.main.wireprotocol.PgQueryAnalyzer.isMetadataQuery;
import static io.wren.main.wireprotocol.PostgresWireProtocolErrorCode.INVALID_PREPARED_STATEMENT_NAME;
import static io.wren.main.wireprotocol.PreparedStatement.RESERVED_DRY_RUN_NAME;
import static io.wren.main.wireprotocol.message.MessageUtils.isIgnoredCommand;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class WireProtocolSession
{
    private static final Logger LOG = Logger.get(WireProtocolSession.class);

    private static final List<Class<?>> SESSION_COMMAND = ImmutableList.of(Deallocate.class);

    private static final String ALL = "all";

    private Properties properties;
    private final PreparedStatementMap preparedStatements = new PreparedStatementMap();
    private final PortalMap portals = new PortalMap();
    private final List<String> sessionProperties = new ArrayList<>();
    private CompletableFuture<Optional<GenericTableRecordIterable>> runningQuery = CompletableFuture.completedFuture(null);
    private final SqlParser sqlParser;
    private final RegObjectFactory regObjectFactory;
    private final Metadata metadata;

    private final SqlConverter sqlConverter;
    private final WrenConfig wrenConfig;
    private final WrenMetastore wrenMetastore;
    private final CacheManager cacheManager;
    private final CachedTableMapping cachedTableMapping;
    private final Authentication authentication;
    private final PgMetastore pgMetastore;

    public WireProtocolSession(
            RegObjectFactory regObjectFactory,
            Metadata metadata,
            SqlConverter sqlConverter,
            WrenConfig wrenConfig,
            WrenMetastore wrenMetastore,
            CacheManager cacheManager,
            CachedTableMapping cachedTableMapping,
            Authentication authentication,
            PgMetastore pgMetastore)
    {
        this.sqlParser = new SqlParser();
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlConverter = sqlConverter;
        this.wrenConfig = requireNonNull(wrenConfig, "wrenConfig is null");
        this.wrenMetastore = requireNonNull(wrenMetastore, "wrenMetastore is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cachedTableMapping = requireNonNull(cachedTableMapping, "cachedTableMapping is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.pgMetastore = requireNonNull(pgMetastore, "metastore is null");
    }

    public int getParamTypeOid(String statementName, int fieldPosition)
    {
        if (preparedStatements.containsKey(statementName)) {
            return preparedStatements.get(statementName).getParamTypeOids().get(fieldPosition);
        }
        else {
            throw new WrenException(NOT_FOUND, format("prepared statement %s not found", statementName));
        }
    }

    public Portal getPortal(String portalName)
    {
        if (portals.containsKey(portalName)) {
            return portals.get(portalName);
        }
        else {
            throw new WrenException(NOT_FOUND, format("portal %s not found", portalName));
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

    public Optional<String> getClientUser()
    {
        return Optional.ofNullable(properties.getProperty("user"));
    }

    public String getDefaultDatabase()
    {
        return properties.getProperty("database");
    }

    public String getDefaultSchema()
    {
        return Optional.ofNullable(properties.getProperty("search_path"))
                // we only support the first search path to be the default schema
                .orElse(extraFirstSearchPath(properties.getProperty("options")));
    }

    private String extraFirstSearchPath(String options)
    {
        if (options == null) {
            return null;
        }
        String searchPath = null;
        for (String option : options.split(" ")) {
            String[] kv = option.split("=");
            if (kv.length != 2) {
                continue;
            }
            if (kv[0].equalsIgnoreCase("--search_path")) {
                searchPath = kv[1].split(",")[0];
            }
        }
        return searchPath;
    }

    public boolean doAuthentication(String password)
    {
        return getClientUser()
                .filter(name -> authentication.authenticate(name, password))
                .isPresent();
    }

    public boolean isAuthenticationEnabled()
    {
        return authentication.isEnabled();
    }

    public Optional<List<Column>> describePortal(String name)
    {
        Portal portal = getPortal(name);
        if (portal.isMetadataQuery() && portal.getConnectorRecordIterator() != null) {
            return Optional.of(portal.getConnectorRecordIterator().getColumns());
        }

        String oriStmt = portal.getPreparedStatement().getOriginalStatement();
        if (oriStmt.isEmpty() || isIgnoredCommand(oriStmt)) {
            return Optional.empty();
        }

        String sql = sqlConverter.convert(
                portal.getPreparedStatement().getStatement(),
                SessionContext.builder()
                        .setCatalog(getDefaultDatabase())
                        .setSchema(getDefaultSchema())
                        .setEnableDynamic(wrenConfig.getEnableDynamicFields())
                        .build());
        return Optional.of(metadata.describeQuery(sql, portal.getParameters()));
    }

    public Optional<List<Integer>> describeStatement(String name)
    {
        return Optional.ofNullable(preparedStatements.get(name)).map(preparedStatement -> preparedStatement.getParamTypeOids());
    }

    /**
     * Refer to the doc, a prepared statement describe is followed by a RowDescription message.
     * Create another dry-run prepared statement and portal to get the RowDescription message and
     * avoid to effect the real prepared statement.
     */
    public Optional<List<Column>> dryRunAfterDescribeStatement(String statementName, List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes)
    {
        parse(RESERVED_DRY_RUN_NAME, preparedStatements.get(statementName).getOriginalStatement(), preparedStatements.get(statementName).getParamTypeOids());
        bind(RESERVED_DRY_RUN_NAME, RESERVED_DRY_RUN_NAME, params, resultFormatCodes);

        Optional<List<Column>> result = describePortal(RESERVED_DRY_RUN_NAME);
        preparedStatements.remove(RESERVED_DRY_RUN_NAME);
        portals.remove(RESERVED_DRY_RUN_NAME);

        return result;
    }

    public void parse(String statementName, String statement, List<Integer> paramTypes)
    {
        if (statementName.equalsIgnoreCase(ALL)) {
            throw new WrenException(INVALID_PREPARED_STATEMENT_NAME, format("%s is a preserved word. Can't be the name of prepared statement", statementName));
        }

        String statementTrimmed = rewritePreparedChar(statement.split(";")[0].trim());
        if (statementTrimmed.isEmpty() || isIgnoredCommand(statementTrimmed)) {
            preparedStatements.put(statementName, new PreparedStatement(statementName, "", paramTypes, statementTrimmed, false, QueryLevel.DATASOURCE));
            return;
        }
        LOG.info("Parse statement: %s", statementTrimmed);
        // To fit SQL syntax of Wren
        String statementPreRewritten = PostgreSqlRewriteUtil.rewrite(statementTrimmed);
        if (isMetadataQuery(statementPreRewritten)) {
            // Level 1 Query
            createMetadataQueryPreparedStatement(statementName, statement, statement, paramTypes, QueryLevel.METASTORE_FULL);
            return;
        }

        parseDataSourceQuery(statementName, statement, paramTypes);
    }

    private void parseMetastoreSemiQuery(String statementName, String statement, List<Integer> paramTypes)
    {
        String statementTrimmed = rewritePreparedChar(statement.split(";")[0].trim());
        String statementPreRewritten = PostgreSqlRewriteUtil.rewrite(statementTrimmed);
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog(getDefaultDatabase())
                .setSchema(getDefaultSchema())
                .setEnableDynamic(wrenConfig.getEnableDynamicFields())
                .build();
        try {
            Statement metadataQueryStatement = MetastoreSqlRewrite.rewrite(regObjectFactory,
                    parseSql(statementPreRewritten));
            String converted = pgMetastore.getSqlConverter().convert(SqlFormatter.formatSql(metadataQueryStatement), sessionContext);
            createMetadataQueryPreparedStatement(statementName, statement, converted, paramTypes, QueryLevel.METASTORE_SEMI);
        }
        catch (Exception e) {
            LOG.debug(e, "Failed to parse SQL in METASTORE_SEMI level: %s", statement);
        }
    }

    private void parseDataSourceQuery(String statementName, String statement, List<Integer> paramTypes)
    {
        String statementTrimmed = rewritePreparedChar(statement.split(";")[0].trim());
        // To fit SQL syntax of Wren
        String statementPreRewritten = PostgreSqlRewriteUtil.rewrite(statementTrimmed);
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog(getDefaultDatabase())
                .setSchema(getDefaultSchema())
                .setEnableDynamic(wrenConfig.getEnableDynamicFields())
                .build();
        AnalyzedMDL analyzedMDL = wrenMetastore.getAnalyzedMDL();
        String wrenRewritten = WrenPlanner.rewrite(
                statementPreRewritten,
                sessionContext,
                analyzedMDL);
        // TODO: support set session property
        // validateSetSessionProperty(statementPreRewritten);
        Statement parsedStatement = parseSql(wrenRewritten);
        Statement rewrittenStatement = PostgreSqlRewrite.rewrite(regObjectFactory, metadata.getDefaultCatalog(), metadata.getPgCatalogName(), parsedStatement);
        List<Integer> rewrittenParamTypes = rewriteParameters(rewrittenStatement, paramTypes);
        preparedStatements.put(statementName,
                new PreparedStatement(
                        statementName,
                        getFormattedSql(rewrittenStatement, sqlParser),
                        CacheRewrite.rewrite(sessionContext, statementPreRewritten, cachedTableMapping::convertToCachedTable, analyzedMDL.getWrenMDL()),
                        rewrittenParamTypes,
                        statementTrimmed,
                        isSessionCommand(rewrittenStatement),
                        QueryLevel.DATASOURCE));
        LOG.info("Create preparedStatement %s", statementName);
    }

    private void createMetadataQueryPreparedStatement(String statementName, String statement, String rewritten, List<Integer> paramTypes, QueryLevel level)
    {
        PreparedStatement preparedStatement = new PreparedStatement(statementName, rewritten, paramTypes, statement, false, level);
        preparedStatements.put(statementName, preparedStatement);
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
        PreparedStatement preparedStatement = preparedStatements.get(statementName);
        if (preparedStatement.isMetaDtaQuery()) {
            try {
                Portal portal = PostgreSqlRewriteUtil.rewriteWithParameters(new Portal(portalName, preparedStatement, params, resultFormatCodes));
                // Execute Level 1 Query
                LOG.debug("Bind Portal %s with parameters %s to Statement %s", portalName, params.stream().map(Object::toString).collect(Collectors.joining(",")), statementName);
                ConnectorRecordIterator iter = pgMetastore.directQuery(portal.getPreparedStatement().getStatement(), portal.getParameters());
                portal.setConnectorRecordIterator(iter);
                portals.put(portalName, portal);
                return;
            }
            catch (Exception e) {
                // Forward to level 2
                LOG.debug("Failed to execute SQL in METASTORE_FULL level: %s", preparedStatement.getStatement());
                parseMetastoreSemiQuery(preparedStatement.getName(),
                        preparedStatement.getOriginalStatement(),
                        preparedStatement.getParamTypeOids());
            }
        }

        // Execute Level 2 Query
        preparedStatement = preparedStatements.get(statementName);
        if (preparedStatement.isMetaDtaQuery()) {
            try {
                Portal portal = new Portal(portalName, preparedStatement, params, resultFormatCodes);
                ConnectorRecordIterator iter = pgMetastore.directQuery(portal.getPreparedStatement().getStatement(), portal.getParameters());
                portal.setConnectorRecordIterator(iter);
                portals.put(portalName, portal);
                return;
            }
            catch (Exception e) {
                // Forward to level 3
                LOG.debug(e, "Failed to execute SQL in METASTORE_SEMI level: %s", preparedStatement.getStatement());
                parseDataSourceQuery(preparedStatement.getName(),
                        preparedStatement.getOriginalStatement(),
                        preparedStatement.getParamTypeOids());
            }
        }

        // Bind Level 3 Query
        portals.put(portalName, new Portal(portalName, preparedStatements.get(statementName), params, resultFormatCodes));
        String paramString = params.stream()
                .map(element -> (isNull(element)) ? "null" : element.toString())
                .collect(Collectors.joining(","));
        LOG.info("Bind Portal %s with parameters %s to Statement %s", portalName, paramString, statementName);
    }

    public CompletableFuture<Optional<ConnectorRecordIterator>> execute(String portalName)
    {
        return execute(portals.get(portalName));
    }

    private CompletableFuture<Optional<ConnectorRecordIterator>> execute(Portal portal)
    {
        if (portal.isMetadataQuery()) {
            return CompletableFuture.completedFuture(Optional.of(portal.getConnectorRecordIterator()));
        }

        String execStmt = portal.getPreparedStatement().getStatement();
        return CompletableFuture.supplyAsync(() -> executeCache(portal).or(() -> {
            String sql = sqlConverter.convert(execStmt,
                    SessionContext.builder()
                            .setCatalog(getDefaultDatabase())
                            .setSchema(getDefaultSchema())
                            .setEnableDynamic(wrenConfig.getEnableDynamicFields())
                            .build());
            return Optional.of(metadata.directQuery(sql, portal.getParameters()));
        }));
    }

    private Optional<ConnectorRecordIterator> executeCache(Portal portal)
    {
        return portal.getPreparedStatement().getCacheStatement().map(statement -> {
            try {
                return cacheManager.query(statement, portal.getParameters());
            }
            catch (Exception e) {
                LOG.warn(e, "Failed to execute cache query: %s", statement);
                return null;
            }
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

    public void close(Close.CloseType type, String name)
            throws Exception
    {
        switch (type) {
            case PORTAL:
                Optional.ofNullable(portals.get(name)).ifPresent(portal -> {
                    portals.remove(name);
                });
                LOG.info("Close portal %s", name);
                break;
            case STATEMENT:
                Optional.ofNullable(preparedStatements.get(name)).ifPresent(preparedStatement -> {
                    preparedStatements.remove(name);
                    List<String> removedNames = portals.entrySet().stream()
                            .filter(entry -> entry.getValue().getPreparedStatement().getName().equals(preparedStatement.getName()))
                            .map(Map.Entry::getKey).collect(toImmutableList());
                    removedNames.forEach(portals::remove);
                });
                LOG.info("Close statement %s", name);
                break;
            default:
                throw new WrenException(INVALID_PARAMETER_USAGE, format("Type %s is invalid. We only support 'P' and 'S'", type));
        }
    }

    private static class PreparedStatementMap
    {
        private final HashMap<String, PreparedStatement> delegate = new HashMap<>();

        public PreparedStatement get(String key)
        {
            if (key.isEmpty()) {
                return delegate.get(PreparedStatement.RESERVED_PREPARE_NAME);
            }
            return delegate.get(key);
        }

        public PreparedStatement put(String key, PreparedStatement value)
        {
            if (key.isEmpty()) {
                return delegate.put(PreparedStatement.RESERVED_PREPARE_NAME, value);
            }
            return delegate.put(key, value);
        }

        public void remove(String key)
        {
            if (key.isEmpty()) {
                delegate.remove(PreparedStatement.RESERVED_PREPARE_NAME);
            }
            delegate.remove(key);
        }

        public boolean containsKey(String key)
        {
            if (key.isEmpty()) {
                return delegate.containsKey(PreparedStatement.RESERVED_PREPARE_NAME);
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

        public void remove(String key)
        {
            close(key);
            delegate.remove(key);
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
