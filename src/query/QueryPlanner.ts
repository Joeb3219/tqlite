import _ from "lodash";
import { DatabaseFile } from "../file/DatabaseFile";
import {
    ASTKinds,
    binary_operator,
    expression_front_recursive,
    expression_function_invocation,
    select_group_by,
    select_limit,
    select_ordering_term,
    select_qualifier,
    select_result_column,
    select_where,
    stmt_select,
} from "../parser-autogen/parser";
import { ASTUtil, FlattenedSelectFrom } from "./AST.util";
import { ResultSet } from "./QueryPlanner.types";
import { QueryPlannerAggregateFunctions } from "./QueryPlannerAggregateFunctions";
import { QueryPlannerDateFunctions } from "./QueryPlannerDateFunctions";
import { QueryPlannerJoin } from "./QueryPlannerJoin";
import { QueryPlannerMathFunctions } from "./QueryPlannerMathFunctions";
import { QueryPlannerScalarFunctions } from "./QueryPlannerScalarFunctions";

const BinaryOperationMap: {
    [operator in binary_operator["kind"]]: (a: any, b: any) => any;
} = {
    literal_and: (a, b) => a && b,
    literal_or: (a, b) => a || b,
    literal_plus: (a, b) => a + b,
    literal_minus: (a, b) => a - b,
    literal_slash: (a, b) => a / b,
    literal_gte: (a, b) => a >= b,
    literal_lte: (a, b) => a <= b,
    literal_gt: (a, b) => a > b,
    literal_lt: (a, b) => a < b,
    literal_equal: (a, b) => a == b,
    literal_not_equal_1: (a, b) => a != b,
    literal_not_equal_2: (a, b) => a != b,
    literal_asterisk: (a, b) => a * b,
};

export class QueryPlanner {
    constructor(
        private readonly database: DatabaseFile,
        private readonly query: stmt_select
    ) {}

    private fetchResultSetInternal(
        from: FlattenedSelectFrom,
        where?: select_where
    ): ResultSet {
        switch (from.kind) {
            case ASTKinds.select_from_1: {
                const tableA = this.fetchResultSet([from.join.table_a]);
                const joins = from.join.joins.reduce<ResultSet>(
                    (joinState, join) => {
                        const joinTable = this.fetchResultSet(
                            [join.select_from_table_or_subquery],
                            undefined
                        ); // TODO add where
                        const criterion = (row: any): boolean => {
                            if (!join.select_from_join_constraint) {
                                return true;
                            }

                            if (
                                join.select_from_join_constraint.kind ===
                                ASTKinds.select_from_join_constraint_1
                            ) {
                                return !!QueryPlanner.evaluateExpression(
                                    this.database,
                                    row,
                                    join.select_from_join_constraint.expression
                                ).value;
                            }

                            throw new Error("USING clause not yet implemented");
                        };
                        switch (join.select_from_join_operator.type?.kind) {
                            case ASTKinds.literal_natural:
                                throw new Error(
                                    "Natural join not yet implemented"
                                );
                            case ASTKinds.literal_left:
                                return QueryPlannerJoin.leftJoin(
                                    joinState,
                                    joinTable,
                                    criterion
                                );
                            case ASTKinds.literal_right:
                                return QueryPlannerJoin.rightJoin(
                                    joinState,
                                    joinTable,
                                    criterion
                                );
                            case ASTKinds.literal_inner:
                                return QueryPlannerJoin.innerJoin(
                                    joinState,
                                    joinTable,
                                    criterion
                                );
                            case ASTKinds.literal_full:
                                return QueryPlannerJoin.fullJoin(
                                    joinState,
                                    joinTable,
                                    criterion
                                );
                            case ASTKinds.literal_cross:
                                return QueryPlannerJoin.crossJoin(
                                    joinState,
                                    joinTable,
                                    criterion
                                );
                        }
                        return QueryPlannerJoin.innerJoin(
                            joinState,
                            joinTable,
                            criterion
                        );
                    },
                    tableA
                );
                return joins;
            }
            case ASTKinds.select_from_table_or_subquery_1:
                return this.fetchResultSet(
                    ASTUtil.flattenSelectFrom(from.table_or_subquery)
                );
            case ASTKinds.select_from_table_or_subquery_2: {
                const innerSelect = new QueryPlanner(
                    this.database,
                    from.select_stmt
                ).execute();
                const alias = from.alias?.value;
                if (!alias) {
                    throw new Error(
                        "Unnamed from-select clauses not yet supported"
                    );
                }

                return innerSelect.map<ResultSet[number]>((s) => ({
                    [alias]: s,
                }));
            }
            case ASTKinds.select_from_table_or_subquery_3:
                // TODO: handle schemas
                // TODO: properly handle joining with other tables
                // TODO: pre-mature filtering
                return this.database
                    .getRows(from.table_name.value, () => true)
                    .map((row) => ({
                        [from.alias?.value ?? from.table_name.value]: row,
                    }));
        }
    }

    // SELECT * from knex_migrations s ORDER BY s.batch desc, s.name asc LIMIT 6
    fetchResultSet(
        from: FlattenedSelectFrom[],
        where?: select_where
    ): ResultSet {
        return from
            .reduce<ResultSet>((state, f) => {
                const newSource = this.fetchResultSetInternal(f, where);
                return QueryPlannerJoin.innerJoin(state, newSource, () => true);
            }, [])
            .filter((row) =>
                where
                    ? QueryPlanner.evaluateExpression(
                          this.database,
                          row,
                          where.expression
                      ).value
                    : true
            );
    }

    static getTableFromRow(row: any, tableName?: string) {
        if (!tableName) {
            throw new Error("Unnamed tables are not yet supported");
        }

        return row[tableName];
    }

    static evaluateExpression(
        database: DatabaseFile,
        row: any,
        expression: expression_front_recursive,
        alias?: string
    ): { name: string; value: any } {
        switch (expression.kind) {
            // TODO: support schema
            case ASTKinds.expression_column_1:
                return {
                    name:
                        alias ??
                        _.compact([
                            expression.schema_name?.schema_name.value,
                            expression.table_name?.table_name.value,
                            expression.column_name.value,
                        ]).join("."),
                    value: this.getTableFromRow(
                        row,
                        expression.table_name.table_name.value
                    )?.[expression.column_name.value],
                };
            case ASTKinds.expression_column_2:
                return {
                    name:
                        alias ??
                        _.compact([
                            expression.table_name?.table_name.value,
                            expression.column_name.value,
                        ]).join("."),
                    value: this.getTableFromRow(
                        row,
                        expression.table_name?.table_name.value
                    )?.[expression.column_name.value],
                };
            case ASTKinds.expression_unary: {
                const recursiveExpression = QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.expression,
                    alias
                );
                const resolvedName =
                    alias ??
                    `${expression.unary_operator.literal} ${recursiveExpression.name}`;

                if (expression.unary_operator.kind === ASTKinds.literal_not) {
                    return {
                        name: resolvedName,
                        value: !recursiveExpression.value,
                    };
                }
                break;
            }
            case ASTKinds.expression_binary: {
                const recursiveExpressionA = QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.expression_a,
                    alias
                );
                const recursiveExpressionB = QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.expression_b,
                    alias
                );
                const resolvedName =
                    alias ??
                    `${recursiveExpressionA.name} ${expression.operator.literal} ${recursiveExpressionB.name}`;

                return {
                    name: resolvedName,
                    value: BinaryOperationMap[expression.operator.kind](
                        recursiveExpressionA.value,
                        recursiveExpressionB.value
                    ),
                };
            }
            case ASTKinds.expression_null_assertion_1:
            case ASTKinds.expression_null_assertion_2:
            case ASTKinds.expression_null_assertion_3: {
                const recursiveExpression = QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.expression,
                    alias
                );
                const isAssertingNotNull =
                    "not_null" in expression && !!expression.not_null;
                const isExpressionNull =
                    recursiveExpression.value === null ||
                    recursiveExpression.value === undefined;
                return {
                    name:
                        alias ??
                        `${recursiveExpression.name} IS ${
                            isAssertingNotNull ? "NOT NULL" : "NULL"
                        }`,
                    value: isAssertingNotNull
                        ? !isExpressionNull
                        : isExpressionNull,
                };
            }
            case ASTKinds.expression_in: {
                const recursiveExpression = QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.expression,
                    alias
                );
                const expressionList = [
                    expression.values.expression_or_select,
                    ...expression.values.other_expression_or_selects.map(
                        (e) => e.expression_or_select
                    ),
                ];
                const recursiveExpressionList = expressionList.map((e) => {
                    if ("stmt_select" in e) {
                        const innerSelect = new QueryPlanner(
                            database,
                            e.stmt_select
                        ).execute();

                        return innerSelect;
                    }

                    return QueryPlanner.evaluateExpression(
                        database,
                        row,
                        e.expression,
                        alias
                    ).value;
                });

                const isInValues = recursiveExpressionList.includes(
                    recursiveExpression.value
                );

                return {
                    name: `${expression.invert ? "IS NOT" : ""} IN (STMTS)`,
                    value: expression.invert ? !isInValues : isInValues,
                };
            }
            case ASTKinds.expression_between: {
                const recursiveExpressionSource =
                    QueryPlanner.evaluateExpression(
                        database,
                        row,
                        expression.expression,
                        alias
                    );
                const recursiveExpressionLeft = QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.left_expression,
                    alias
                );
                const recursiveExpressionRight =
                    QueryPlanner.evaluateExpression(
                        database,
                        row,
                        expression.right_expression,
                        alias
                    );

                return {
                    name:
                        alias ??
                        `${recursiveExpressionSource.name} IS BETWEEN ${recursiveExpressionLeft.name} AND ${recursiveExpressionRight.name}`,
                    value:
                        recursiveExpressionSource.value >=
                            recursiveExpressionLeft.value &&
                        recursiveExpressionSource.value <=
                            recursiveExpressionRight.value,
                };
            }
            case ASTKinds.expression_exists_assertion: {
                const innerSelect = new QueryPlanner(
                    database,
                    expression.stmt_select
                ).execute();

                return {
                    name:
                        alias ??
                        `${
                            expression.invert ? "NOT EXISTS" : "EXISTS"
                        } (SELECT)`,
                    value: expression.invert
                        ? !innerSelect.length
                        : !!innerSelect.length,
                };
            }
            case ASTKinds.expression_parens:
                return QueryPlanner.evaluateExpression(
                    database,
                    row,
                    expression.expression,
                    alias
                );
            case ASTKinds.expression_function_invocation: {
                const recursiveExpressions = expression.expression_list
                    ? [
                          expression.expression_list.expression,
                          ...expression.expression_list.other_expressions.map(
                              (e) => e.expression
                          ),
                      ].map(
                          (e) =>
                              QueryPlanner.evaluateExpression(
                                  database,
                                  row,
                                  e,
                                  alias
                              ).value
                      )
                    : [];

                const fnName = expression.function_name.value.toLowerCase();
                const value = QueryPlannerScalarFunctions.hasFunction(fnName)
                    ? QueryPlannerScalarFunctions.executeFunction(
                          fnName,
                          recursiveExpressions
                      )
                    : QueryPlannerMathFunctions.hasFunction(fnName)
                    ? QueryPlannerMathFunctions.executeFunction(
                          fnName,
                          recursiveExpressions
                      )
                    : QueryPlannerDateFunctions.hasFunction(fnName)
                    ? QueryPlannerDateFunctions.executeFunction(
                          fnName,
                          recursiveExpressions
                      )
                    : undefined;

                return {
                    name: alias ?? `${fnName}(EXPRESSIONS)`,
                    value,
                };
            }
            case ASTKinds.identifier:
                throw new Error("Unexpected column type identifier");
            case ASTKinds.num:
                return {
                    name: alias ?? expression.value,
                    value: parseFloat(expression.value),
                };
            case ASTKinds.quoted_string: {
                const unquotedString = expression.value.substring(
                    1,
                    expression.value.length - 1
                );
                return {
                    name: alias ?? unquotedString,
                    value: unquotedString,
                };
            }
            case ASTKinds.literal_true:
                return {
                    name: alias ?? "true",
                    value: true,
                };
            case ASTKinds.literal_false:
                return {
                    name: alias ?? "false",
                    value: false,
                };
            case ASTKinds.literal_null:
                return {
                    name: alias ?? "null",
                    value: null,
                };
        }

        throw new Error("Unknown expression");
    }

    static isExpressionAnAggregateExpression(
        expression: expression_front_recursive
    ): expression is expression_function_invocation {
        if (expression.kind !== ASTKinds.expression_function_invocation) {
            return false;
        }

        const functionName = expression.function_name.value;
        return QueryPlannerAggregateFunctions.hasFunction(functionName);
    }

    static evaluateExpressionOrAggregate(
        database: DatabaseFile,
        rows: any[],
        expression: expression_front_recursive,
        alias?: string
    ): { name: string; value: any } {
        if (!QueryPlanner.isExpressionAnAggregateExpression(expression)) {
            return QueryPlanner.evaluateExpression(
                database,
                rows[0],
                expression,
                alias
            );
        }

        const functionName = expression.function_name.value;
        const expressions = expression.expression_list
            ? [
                  expression.expression_list.expression,
                  ...expression.expression_list.other_expressions.map(
                      (e) => e.expression
                  ),
              ]
            : [];

        const recursiveExpressions = expressions.map((e) => {
            return rows.map(
                (row) =>
                    QueryPlanner.evaluateExpressionOrAggregate(
                        database,
                        [row],
                        e,
                        alias
                    ).value
            );
        });

        return {
            name: alias ?? `${functionName}(AGG)`,
            value: QueryPlannerAggregateFunctions.executeFunction(
                functionName,
                rows,
                recursiveExpressions
            ),
        };
    }

    groupResults(resultSet: ResultSet, groupBy: select_group_by): ResultSet[] {
        const expressions = [
            groupBy.expression_list.expression,
            ...groupBy.expression_list.other_expressions.map(
                (e) => e.expression
            ),
        ];
        const resultSetGroups = resultSet.reduce<
            { resultSet: ResultSet; key: any[] }[]
        >((state, row) => {
            const evaluatedExpressions = expressions.map(
                (e) =>
                    QueryPlanner.evaluateExpression(this.database, row, e).value
            );

            // Now we attempt to find an island with all of these fields
            const foundIslandIndex = state.findIndex((island) =>
                _.isEqual(island.key, evaluatedExpressions)
            );
            if (foundIslandIndex === -1) {
                return [
                    ...state,
                    {
                        key: evaluatedExpressions,
                        resultSet: [row],
                    },
                ];
            }

            return state.map((island, idx) =>
                idx === foundIslandIndex
                    ? {
                          key: island.key,
                          resultSet: [...island.resultSet, row],
                      }
                    : island
            );
        }, []);

        const groupByExpression = groupBy.having?.expression;
        if (groupByExpression) {
            const filteredGroups = resultSetGroups.filter((group) => {
                const recursiveExpression =
                    QueryPlanner.evaluateExpressionOrAggregate(
                        this.database,
                        group.resultSet,
                        groupByExpression
                    );
                return !!recursiveExpression.value;
            });

            return filteredGroups.map((f) => f.resultSet);
        }

        return resultSetGroups.map((f) => f.resultSet);
    }

    // SELECT soundex(m.name), random(), datetime("now", "start of year", "-10040 days", "4443434330.4343430 seconds"), m.name, * FROM knex_migrations m JOIN playsets_mods p
    selectColumns(
        resultSet: ResultSet,
        columns: select_result_column[],
        qualifier?: select_qualifier,
        groupBy?: select_group_by
    ): any[] {
        const hasAggregateColumn = columns.some((column) =>
            column.kind === ASTKinds.select_result_column_expression
                ? QueryPlanner.isExpressionAnAggregateExpression(
                      column.expression
                  )
                : false
        );
        const groupedRows: ResultSet[] = groupBy
            ? this.groupResults(resultSet, groupBy)
            : hasAggregateColumn
            ? [resultSet]
            : resultSet.map((r) => [r]);

        const projection = groupedRows.map((group) => {
            // For operations that require a single row, we choose one arbitarily.
            const firstRow = group[0];
            return columns.reduce((state, column) => {
                if (column.kind === ASTKinds.literal_asterisk) {
                    return {
                        ...state,
                        ...Object.entries(firstRow).reduce(
                            (innerState, [_tableName, data]) => {
                                return { ...innerState, ...data };
                            },
                            {}
                        ),
                    };
                }

                if (column.kind === ASTKinds.select_result_column_whole_table) {
                    return {
                        ...state,
                        ...firstRow[column.table_name.value],
                    };
                }

                const { name, value } =
                    QueryPlanner.evaluateExpressionOrAggregate(
                        this.database,
                        group,
                        column.expression,
                        column.column_alias?.value
                    );
                return {
                    ...state,
                    [name]: value,
                };
            }, {});
        });

        // If the user requests distinct, we filter out any repeats
        return qualifier?.kind === ASTKinds.literal_distinct
            ? _.uniqWith(projection, _.isEqual)
            : projection;
    }

    applyOrdering(rows: ResultSet, orderBy: select_ordering_term[]): ResultSet {
        const sorted = rows.sort((a, b) => {
            return orderBy.reduce<-1 | 0 | 1>((state, term) => {
                // We don't have to evaluate this column if the previous column wasn't equal
                if (state !== 0) {
                    return state;
                }

                const isAscending =
                    term.sort_direction?.value.kind !== ASTKinds.literal_desc;
                const evalA = QueryPlanner.evaluateExpression(
                    this.database,
                    a,
                    term.expression
                );
                const evalB = QueryPlanner.evaluateExpression(
                    this.database,
                    b,
                    term.expression
                );

                // We're the same, let's look at the next column.
                if (evalA.value == evalB.value) {
                    return 0;
                }

                if (
                    evalA.value === null &&
                    evalB.value !== null &&
                    term.nulls_direction
                ) {
                    return term.nulls_direction.value.kind ===
                        ASTKinds.literal_first
                        ? 1
                        : -1;
                }

                if (
                    evalA.value !== null &&
                    evalB.value === null &&
                    term.nulls_direction
                ) {
                    return term.nulls_direction.value.kind ===
                        ASTKinds.literal_first
                        ? -1
                        : 1;
                }

                if (evalA.value < evalB.value && term.sort_direction) {
                    return isAscending ? -1 : 1;
                }

                return isAscending ? 1 : -1;
            }, 0);
        });

        return sorted;
    }

    applyLimit(rows: any[], limit: select_limit): any[] {
        // TODO: handle non-number offsets and limits
        const offsetValue =
            limit.offset?.offset?.kind === ASTKinds.num
                ? parseInt(limit.offset.offset.value)
                : 0;
        const limitValue =
            limit.expression.kind === ASTKinds.num
                ? parseInt(limit.expression.value)
                : 0;
        return rows.slice(offsetValue, limitValue + offsetValue);
    }

    execute(): any[] {
        const { where, group_by, limit, qualifier } = this.query.select_core;
        const columns = ASTUtil.flattenSelectRealColumnList(
            this.query.select_core.columns
        );
        const from = this.query.select_core.from
            ? ASTUtil.flattenSelectFrom(this.query.select_core.from)
            : [];
        const orderBy = this.query.select_core.order_by
            ? ASTUtil.flattenOrderByTermList(
                  this.query.select_core.order_by.select_ordering_term_list
              )
            : [];

        const resultSet = this.fetchResultSet(from, where ?? undefined);
        const orderedResultSet = this.applyOrdering(resultSet, orderBy);
        const selectionAppliedRows = this.selectColumns(
            orderedResultSet,
            columns,
            qualifier ?? undefined,
            group_by ?? undefined
        );

        return limit
            ? this.applyLimit(selectionAppliedRows, limit)
            : selectionAppliedRows;
    }
}
