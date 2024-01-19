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

package io.accio.main.wireprotocol;

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioException;
import io.accio.base.AnalyzedMDL;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.SessionContext;
import io.accio.base.sql.SqlConverter;
import io.accio.base.sqlrewrite.AccioPlanner;
import io.accio.base.sqlrewrite.CacheRewrite;
import io.accio.cache.CacheManager;
import io.accio.cache.CachedTableMapping;
import io.accio.main.AccioConfig;
import io.accio.main.AccioMetastore;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.regtype.RegObjectFactory;
import io.accio.main.sql.PostgreSqlRewrite;
import io.accio.main.wireprotocol.auth.Authentication;
import io.accio.main.wireprotocol.patterns.PostgreSqlRewriteUtil;
import io.airlift.log.Logger;
import io.trino.sql.SqlFormatter;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.metadata.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.accio.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.accio.main.wireprotocol.PgQueryAnalyzer.isMetadataQuery;
import static io.accio.main.wireprotocol.PostgresWireProtocol.isIgnoredCommand;
import static io.accio.main.wireprotocol.PostgresWireProtocolErrorCode.INVALID_PREPARED_STATEMENT_NAME;
import static io.accio.main.wireprotocol.WireProtocolSession.PreparedStmtPortalName.preparedStmtPortalName;
import static io.accio.main.wireprotocol.patterns.PostgreSqlRewriteUtil.rewriteWithParameters;
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

    public static final ParsingOptions PARSE_AS_DECIMAL = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private static final String ALL = "all";

    private Properties properties;
    private final PreparedStatementMap preparedStatements = new PreparedStatementMap();
    private final PortalMap portals = new PortalMap();
    private final List<String> sessionProperties = new ArrayList<>();
    private CompletableFuture<Optional<GenericTableRecordIterable>> runningQuery = CompletableFuture.completedFuture(null);
    private final HashMap<PreparedStmtPortalName, Query> queries = new HashMap<>();
    private final SqlParser sqlParser;
    private final RegObjectFactory regObjectFactory;
    private final Metadata metadata;

    private final SqlConverter sqlConverter;
    private final AccioConfig accioConfig;
    private final AccioMetastore accioMetastore;
    private final CacheManager cacheManager;
    private final CachedTableMapping cachedTableMapping;
    private final Authentication authentication;
    private final PgMetastore pgMetastore;

    public WireProtocolSession(
            RegObjectFactory regObjectFactory,
            Metadata metadata,
            SqlConverter sqlConverter,
            AccioConfig accioConfig,
            AccioMetastore accioMetastore,
            CacheManager cacheManager,
            CachedTableMapping cachedTableMapping,
            Authentication authentication,
            PgMetastore pgMetastore)
    {
        this.sqlParser = new SqlParser();
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlConverter = sqlConverter;
        this.accioConfig = requireNonNull(accioConfig, "accioConfig is null");
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
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
            throw new AccioException(NOT_FOUND, format("prepared statement %s not found", statementName));
        }
    }

    public Portal getPortal(String portalName)
    {
        if (portals.containsKey(portalName)) {
            return portals.get(portalName);
        }
        else {
            throw new AccioException(NOT_FOUND, format("portal %s not found", portalName));
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

    public Optional<List<Column>> describePortal(String name)
    {
        Portal portal = getPortal(name);
        Query query = queries.get(preparedStmtPortalName(portal.getPreparedStatement().getName(), name));
        if (query != null && query.getConnectorRecordIterator().isPresent()) {
            return Optional.of(query.getConnectorRecordIterator().get().getColumns());
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
                        .setEnableDynamic(accioConfig.getEnableDynamicFields())
                        .build());
        return Optional.of(metadata.describeQuery(sql, portal.getParameters()));
    }

    public List<Integer> describeStatement(String name)
    {
        return preparedStatements.get(name).getParamTypeOids();
    }

    public void parse(String statementName, String statement, List<Integer> paramTypes)
    {
        if (statementName.equalsIgnoreCase(ALL)) {
            throw new AccioException(INVALID_PREPARED_STATEMENT_NAME, format("%s is a preserved word. Can't be the name of prepared statement", statementName));
        }

        String statementTrimmed = rewritePreparedChar(statement.split(";")[0].trim());
        if (statementTrimmed.isEmpty() || isIgnoredCommand(statementTrimmed)) {
            preparedStatements.put(statementName, new PreparedStatement(statementName, "", paramTypes, statementTrimmed, false, QueryLevel.DATASOURCE));
            return;
        }
        LOG.info("Parse statement: %s", statementTrimmed);
        // To fit SQL syntax of Accio
        String statementPreRewritten = PostgreSqlRewriteUtil.rewrite(statementTrimmed);
        if (isMetadataQuery(statementPreRewritten)) {
            // Level 1 Query
            // PG Metadata Query won't carry any parameters
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
                .setEnableDynamic(accioConfig.getEnableDynamicFields())
                .build();
        try {
            Statement metadataQueryStatement = MetastoreSqlRewrite.rewrite(regObjectFactory,
                    sqlParser.createStatement(statementPreRewritten, PARSE_AS_DECIMAL));
            LOG.info("Rewritten SQL: %s", SqlFormatter.formatSql(metadataQueryStatement));
            String converted = pgMetastore.getSqlConverter().convert(SqlFormatter.formatSql(metadataQueryStatement), sessionContext);
            LOG.info(converted);
            createMetadataQueryPreparedStatement(statementName, statement, converted, paramTypes, QueryLevel.METASTORE_SEMI);
        }
        catch (Exception e) {
            LOG.debug(e, "Failed to parse SQL in METASTORE_SEMI level: %s", statement);
        }
    }

    private void parseDataSourceQuery(String statementName, String statement, List<Integer> paramTypes)
    {
        String statementTrimmed = rewritePreparedChar(statement.split(";")[0].trim());
        // To fit SQL syntax of Accio
        String statementPreRewritten = PostgreSqlRewriteUtil.rewrite(statementTrimmed);
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog(getDefaultDatabase())
                .setSchema(getDefaultSchema())
                .setEnableDynamic(accioConfig.getEnableDynamicFields())
                .build();
        AnalyzedMDL analyzedMDL = accioMetastore.getAnalyzedMDL();
        String accioRewritten = AccioPlanner.rewrite(
                statementPreRewritten,
                sessionContext,
                analyzedMDL);
        // validateSetSessionProperty(statementPreRewritten);
        Statement parsedStatement = sqlParser.createStatement(accioRewritten, PARSE_AS_DECIMAL);
        Statement rewrittenStatement = PostgreSqlRewrite.rewrite(regObjectFactory, metadata.getDefaultCatalog(), metadata.getPgCatalogName(), parsedStatement);
        List<Integer> rewrittenParamTypes = rewriteParameters(rewrittenStatement, paramTypes);
        preparedStatements.put(statementName,
                new PreparedStatement(
                        statementName,
                        getFormattedSql(rewrittenStatement, sqlParser),
                        CacheRewrite.rewrite(sessionContext, statementPreRewritten, cachedTableMapping::convertToCachedTable, analyzedMDL.getAccioMDL()),
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
        queries.put(preparedStmtPortalName(statementName, null), Query.builder(preparedStatement).build());
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
        Query query = queries.get(preparedStmtPortalName(statementName, null));
        if (query != null) {
            try {
                Portal portal = rewriteWithParameters(new Portal(portalName, query.getPreparedStatement(), params, resultFormatCodes, query.getPreparedStatement().getQueryLevel()));
                query.setPortal(portal);
                // Execute Level 1 Query
                LOG.debug("Bind Portal %s with parameters %s to Statement %s", portalName, params.stream().map(Object::toString).collect(Collectors.joining(",")), statementName);
                ConnectorRecordIterator iter = pgMetastore.directQuery(portal.getPreparedStatement().getStatement(), portal.getParameters());
                query.setConnectorRecordIterator(iter);
                portals.put(portalName, portal);
                queries.remove(preparedStmtPortalName(statementName, null));
                queries.put(preparedStmtPortalName(statementName, portalName), query);
                return;
            }
            catch (Exception e) {
                LOG.debug(e, "Failed to execute SQL in METASTORE_FULL level: %s", query.getPortal().get().getPreparedStatement().getStatement());
                parseMetastoreSemiQuery(query.getPreparedStatement().getName(),
                        query.getPreparedStatement().getOriginalStatement(),
                        query.getPreparedStatement().getParamTypeOids());
            }
        }

        // Execute Level 2 Query
        Query level2Query = queries.get(preparedStmtPortalName(statementName, null));
        if (level2Query != null) {
            try {
                Portal portal = new Portal(portalName, level2Query.getPreparedStatement(), params, resultFormatCodes, level2Query.getPreparedStatement().getQueryLevel());
                level2Query.setPortal(portal);
                ConnectorRecordIterator iter = pgMetastore.directQuery(portal.getPreparedStatement().getStatement(), portal.getParameters());
                level2Query.setConnectorRecordIterator(iter);
                portals.put(portalName, portal);
                queries.remove(preparedStmtPortalName(statementName, null));
                queries.put(preparedStmtPortalName(statementName, portalName), level2Query);
                return;
            }
            catch (Exception e) {
                LOG.debug(e, "Failed to execute SQL in METASTORE_SEMI level: %s", level2Query.getPreparedStatement().getStatement());
                parseDataSourceQuery(level2Query.getPreparedStatement().getName(),
                        level2Query.getPreparedStatement().getOriginalStatement(),
                        level2Query.getPreparedStatement().getParamTypeOids());
            }
        }

        // Bind Level 3 Query

        portals.put(portalName, new Portal(portalName, preparedStatements.get(statementName), params, resultFormatCodes, QueryLevel.DATASOURCE));
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
        Query query = queries.get(preparedStmtPortalName(portal.getPreparedStatement().getName(), portal.getName()));
        if (query != null && query.getConnectorRecordIterator().isPresent()) {
            return CompletableFuture.completedFuture(query.getConnectorRecordIterator());
        }

        String execStmt = portal.getPreparedStatement().getStatement();
        return CompletableFuture.supplyAsync(() -> executeCache(portal).or(() -> {
            String sql = sqlConverter.convert(execStmt,
                    SessionContext.builder()
                            .setCatalog(getDefaultDatabase())
                            .setSchema(getDefaultSchema())
                            .setEnableDynamic(accioConfig.getEnableDynamicFields())
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
        queries.clear();
        return ended;
    }

    public void close(byte type, String name)
    {
        switch (type) {
            case 'P':
                Portal portal = portals.get(name);
                if (portal != null) {
                    portals.remove(name);
                    queries.remove(preparedStmtPortalName(portal.getPreparedStatement().getName(), name));
                }
                break;
            case 'S':
                PreparedStatement preparedStatement = preparedStatements.remove(name);
                if (preparedStatement != null) {
                    List<String> removedNames = portals.entrySet().stream()
                            .filter(entry -> entry.getValue().getPreparedStatement().getName().equals(preparedStatement.getName()))
                            .map(Map.Entry::getKey).collect(toImmutableList());
                    removedNames.forEach(portals::remove);
                    removedNames.forEach(portalName -> queries.remove(preparedStmtPortalName(name, portalName)));
                }
                break;
            default:
                throw new AccioException(INVALID_PARAMETER_USAGE, format("Type %s is invalid. We only support 'P' and 'S'", type));
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

        public PreparedStatement remove(String key)
        {
            if (key.isEmpty()) {
                return delegate.remove(PreparedStatement.RESERVED_PREPARE_NAME);
            }
            return delegate.remove(key);
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

    static class PreparedStmtPortalName
    {
        static PreparedStmtPortalName preparedStmtPortalName(String preparedStmtName, String portalName)
        {
            return new PreparedStmtPortalName(preparedStmtName, portalName);
        }

        private final String preparedStmtName;
        private final String portalName;

        private PreparedStmtPortalName(String preparedStmtName, String portalName)
        {
            this.preparedStmtName = preparedStmtName.isEmpty() ? PreparedStatement.RESERVED_PREPARE_NAME : preparedStmtName;
            this.portalName = portalName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PreparedStmtPortalName that = (PreparedStmtPortalName) o;
            return Objects.equals(preparedStmtName, that.preparedStmtName) && Objects.equals(portalName, that.portalName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(preparedStmtName, portalName);
        }
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
