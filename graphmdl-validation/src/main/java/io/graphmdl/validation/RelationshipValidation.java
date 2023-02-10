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

package io.graphmdl.validation;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.connector.AutoCloseableIterator;
import io.graphmdl.connector.Client;
import io.graphmdl.validation.exception.NotFoundException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RelationshipValidation
        extends ValidationRule
{
    public static final RelationshipValidation RELATIONSHIP_VALIDATION = new RelationshipValidation();
    private static final String RULE_NAME = "relationship";

    @Override
    public List<CompletableFuture<ValidationResult>> validate(Client client, GraphMDL graphMDL)
    {
        return graphMDL.listRelationships().stream().map(relationship -> validateRelationship(client, relationship, graphMDL.listModels()))
                .collect(toUnmodifiableList());
    }

    private CompletableFuture<ValidationResult> validateRelationship(Client client, Relationship relationship, List<Model> models)
    {
        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            try {
                String sql = generateValidationSql(relationship, models);
                try (AutoCloseableIterator<Object[]> iterator = client.query(sql)) {
                    if (iterator.hasNext()) {
                        Object[] row = iterator.next();
                        if ((boolean) row[0]) {
                            return ValidationResult.pass(ValidationResult.formatRuleWithIdentifier(getJoinTypeRuleName(relationship.getJoinType()), relationship.getName()),
                                    Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS));
                        }

                        List<String> invalidModels = new ArrayList<>();
                        // left table failed
                        if (!(boolean) row[1]) {
                            invalidModels.add(relationship.getModels().get(0));
                        }
                        // right table failed
                        if (!(boolean) row[2]) {
                            invalidModels.add(relationship.getModels().get(1));
                        }
                        return ValidationResult.fail(ValidationResult.formatRuleWithIdentifier(getJoinTypeRuleName(relationship.getJoinType()), relationship.getName()),
                                Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS), buildFailMessage(invalidModels));
                    }
                    return ValidationResult.error(ValidationResult.formatRuleWithIdentifier(getJoinTypeRuleName(relationship.getJoinType()), relationship.getName()),
                            Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS), "Query executed failed");
                }
            }
            catch (NotFoundException e) {
                return ValidationResult.fail(ValidationResult.formatRuleWithIdentifier(getJoinTypeRuleName(relationship.getJoinType()), relationship.getName()),
                        Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS), e.getMessage());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String getJoinTypeRuleName(JoinType joinType)
    {
        return RULE_NAME + "_" + joinType;
    }

    private String buildFailMessage(List<String> invalidModels)
    {
        return format("Got duplicate join key in %s", join(",", invalidModels));
    }

    private String generateValidationSql(Relationship relationship, List<Model> models)
            throws NotFoundException
    {
        switch (relationship.getJoinType()) {
            case ONE_TO_ONE:
                return validateOneToOne(relationship, models);
            case MANY_TO_ONE:
                return validateManyToOne(relationship, models);
            case ONE_TO_MANY:
                return validateOneToMany(relationship, models);
            case MANY_TO_MANY:
                return validateManyToMany();
        }
        throw new IllegalArgumentException("Illegal JoinType: " + relationship.getJoinType());
    }

    private String validateOneToOne(Relationship relationship, List<Model> models)
            throws NotFoundException
    {
        ComparisonExpression expression = getConditionNode(relationship.getCondition());
        return format("WITH lefttable AS (%s), righttable AS (%s) SELECT lefttable.result AND righttable.result, lefttable.result, righttable.result FROM lefttable, righttable",
                buildColumnUniqueValidationSql((DereferenceExpression) expression.getLeft(), models),
                buildColumnUniqueValidationSql((DereferenceExpression) expression.getRight(), models));
    }

    private String validateManyToOne(Relationship relationship, List<Model> models)
            throws NotFoundException
    {
        ComparisonExpression expression = getConditionNode(relationship.getCondition());
        return format("WITH righttable AS (%s) SELECT righttable.result, true, righttable.result FROM righttable",
                buildColumnUniqueValidationSql((DereferenceExpression) expression.getRight(), models));
    }

    private String validateOneToMany(Relationship relationship, List<Model> models)
            throws NotFoundException
    {
        ComparisonExpression expression = getConditionNode(relationship.getCondition());

        return format("WITH lefttable AS (%s) SELECT lefttable.result, lefttable.result, true FROM lefttable",
                buildColumnUniqueValidationSql((DereferenceExpression) expression.getLeft(), models));
    }

    private String validateManyToMany()
    {
        return "SELECT true";
    }

    private String buildColumnUniqueValidationSql(DereferenceExpression expression, List<Model> models)
            throws NotFoundException
    {
        Model model = models.stream().filter(m -> m.getName().equals(expression.getBase().toString()))
                .findFirst().orElseThrow(() -> new NotFoundException(expression.getBase().toString() + " model is not found"));
        return format("SELECT count(*) = count(distinct %s) AS result FROM (%s)", expression.getField(), model.getRefSql());
    }

    private ComparisonExpression getConditionNode(String condition)
    {
        SqlParser sqlParser = new SqlParser();
        Query statement = (Query) sqlParser.createStatement("SELECT " + condition, new ParsingOptions(AS_DOUBLE));
        return (ComparisonExpression)
                ((SingleColumn) ((QuerySpecification) statement.getQueryBody()).getSelect().getSelectItems().get(0)).getExpression();
    }
}
