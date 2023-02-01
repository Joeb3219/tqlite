import _ from "lodash";
import { DatabaseFile } from "../file/DatabaseFile";
import { ASTKinds, expression_front_recursive, select_result_column, select_where, stmt_select, binary_operator } from "../parser-autogen/parser";
import { BTree } from "../tree/BTree";
import { ASTUtil, FlattenedSelectFrom } from "./AST.util";
import { ResultSet } from "./QueryPlanner.types";
import { QueryPlannerJoin } from "./QueryPlannerJoin";

const BinaryOperationMap: { [operator in binary_operator['kind']]: (a: any, b: any) => any } = {
    literal_and: (a, b) => a && b,
    literal_or: (a, b) => a || b,
    literal_plus: (a, b) => a + b,
    literal_minus: (a, b) => a - b,
    literal_slash: (a, b) => a / b,
    literal_gte: (a, b) => a >= b,
    literal_lte: (a, b) => a <= b,
    literal_gt: (a, b) => a > b,
    literal_lt: (a, b) => a < b,
    literal_equal: (a, b) =>  a == b,
    literal_not_equal_1: (a, b) => a != b,
    literal_not_equal_2: (a, b) => a != b,
    literal_asterisk: (a, b) => a * b
}

export class QueryPlanner {
    constructor(
        private readonly database: DatabaseFile,
        private readonly query: stmt_select
    ) {}

    // SELECT m.name AS mname, s.name as sname, p.modId AS modId FROM knex_migrations m LEFT JOIN sqlite_sequence s ON m.batch = s.seq LEFT JOIN playsets_mods p ON p.enabled != s.batch WHERE s.name = "a"
    fetchResultSet(from: FlattenedSelectFrom[], where?: select_where): ResultSet {
        return from.reduce<ResultSet>((state, f) => {
            switch (f.kind) {
                case ASTKinds.select_from_1: {
                    const tableA = this.fetchResultSet([f.join.table_a]);
                    const joins = f.join.joins.reduce<ResultSet>((joinState, join) => {
                        const joinTable = this.fetchResultSet([join.select_from_table_or_subquery], undefined); // TODO add where
                        const criterion = (row: any): boolean => {
                            if (!join.select_from_join_constraint) {
                                return true;
                            }

                            if (join.select_from_join_constraint.kind === ASTKinds.select_from_join_constraint_1) {
                                return !!this.evaluateExpression(row, join.select_from_join_constraint.expression).value
                            }

                            throw new Error('USING clause not yet implemented');
                        }
                        switch (join.select_from_join_operator.type?.kind) {
                            case ASTKinds.literal_natural:
                                throw new Error('Natural join not yet implemented');
                            case ASTKinds.literal_left:
                                return QueryPlannerJoin.leftJoin(joinState, joinTable, criterion)
                            case ASTKinds.literal_right:
                                return QueryPlannerJoin.rightJoin(joinState, joinTable, criterion)
                            case ASTKinds.literal_inner:
                                return QueryPlannerJoin.innerJoin(joinState, joinTable, criterion)
                            case ASTKinds.literal_full:
                                throw new Error('Full join not yet implemented');
                            case ASTKinds.literal_cross:    
                                throw new Error('Cross join not yet implemented');
                        }
                        return joinState;
                    }, tableA);
                    return joins;
                }
                case ASTKinds.select_from_table_or_subquery_1:
                    return this.fetchResultSet(ASTUtil.flattenSelectFrom(f.table_or_subquery));
                case ASTKinds.select_from_table_or_subquery_2:
                    throw new Error('Select result set is not yet implemented');
                case ASTKinds.select_from_table_or_subquery_3:
                    // TODO: handle schemas
                    // TODO: properly handle joining with other tables
                    // TODO: pre-mature filtering
                    return this.database.getRows(f.table_name.value, () => true).map(row => ({ [f.alias?.value ?? f.table_name.value]: row }))
            }
            return state;
        }, []).filter(row =>  where ? this.evaluateExpression(row, where.expression).value : true);
    }

    getTableFromRow(row: any, tableName?: string) {
        if (!tableName) {
            throw new Error('Unnamed tables are not yet supported');
        }

        return row[tableName];
    }

    evaluateExpression(row: any, expression: expression_front_recursive, alias?: string): { name: string; value: any } {
        switch (expression.kind) {
            // TODO: support schema
            case ASTKinds.expression_column_1:
                return {
                    name: alias ?? _.compact([expression.schema_name?.schema_name.value, expression.table_name?.table_name.value, expression.column_name.value]).join('.'),
                    value: this.getTableFromRow(row, expression.table_name.table_name.value)?.[expression.column_name.value]
                }
            case ASTKinds.expression_column_2:
                return {
                    name: alias ?? _.compact([expression.table_name?.table_name.value, expression.column_name.value]).join('.'),
                    value: this.getTableFromRow(row, expression.table_name?.table_name.value)?.[expression.column_name.value]
                }
            case ASTKinds.expression_unary: {
                const recursiveExpression = this.evaluateExpression(row, expression.expression, alias);
                const resolvedName = alias ?? `${expression.unary_operator.literal} ${recursiveExpression.name}`;

                if (expression.unary_operator.kind === ASTKinds.literal_not) {
                    return {
                        name: resolvedName,
                        value: !recursiveExpression.value
                    }
                }
                break;
            }
            case ASTKinds.expression_binary: {
                const recursiveExpressionA = this.evaluateExpression(row, expression.expression_a, alias);
                const recursiveExpressionB = this.evaluateExpression(row, expression.expression_b, alias);
                const resolvedName = alias ?? `${recursiveExpressionA.name} ${expression.operator.literal} ${recursiveExpressionB.name}`;

                return {
                    name: resolvedName,
                    value: BinaryOperationMap[expression.operator.kind](recursiveExpressionA.value, recursiveExpressionB.value)
                }
            }
            case ASTKinds.expression_parens: 
                return this.evaluateExpression(row, expression.expression, alias);
            case ASTKinds.identifier:
                throw new Error('Unexpected column type identifier');
            case ASTKinds.num:
                return {
                    name: alias ?? expression.value,
                    value: parseFloat(expression.value)
                }
            case ASTKinds.quoted_string: {
                const unquotedString = expression.value.substring(1, expression.value.length - 1);
                return {
                    name: alias ?? unquotedString,
                    value: unquotedString
                }
            }
            case ASTKinds.literal_true:
                return {
                    name: alias ?? 'true',
                    value: true
                }
            case ASTKinds.literal_false:
                return {
                    name: alias ?? 'false',
                    value: false
                }
            case ASTKinds.literal_null:
                return {
                    name: alias ?? 'null',
                    value: null
                }
        }

        throw new Error('Unknown expression');
    }

    selectColumns(resultSet: ResultSet, columns: select_result_column[]): any[] {
        return resultSet.map(row => {
            return columns.reduce((state, column) => {
                if (column.kind === ASTKinds.literal_asterisk) {
                    return { ...state, ...Object.entries(row).reduce((innerState, [_tableName, data]) => {
                        return { ...innerState, ...data };
                    }, {}) 
                };
                }


                if (column.kind === ASTKinds.select_result_column_whole_table) {
                    return {
                        ...state,
                        ...row[column.table_name.value]
                    }
                }

                const { name, value } = this.evaluateExpression(row, column.expression, column.column_alias?.value);
                return {
                    ...state,
                    [name]: value 
                }
            }, {})
        })
    }

    execute(): any[] {
        const { where } = this.query.select_core;
        const columns = ASTUtil.flattenSelectRealColumnList(this.query.select_core.columns);
        const from = this.query.select_core.from ? ASTUtil.flattenSelectFrom(this.query.select_core.from) : [];

        const resultSet = this.fetchResultSet(from, where ?? undefined);    
        return this.selectColumns(resultSet, columns);     
    }
}
