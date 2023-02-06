import _ from "lodash";
import { DatabaseFile } from "../file/DatabaseFile";
import {
    ASTKinds,
    column_constraint_default,
    expression_front_recursive,
    identifier,
    stmt_insert,
    stmt_insert_core,
    stmt_insert_core_values,
    value_literal,
} from "../parser-autogen/parser";
import {
    TableColumnDefinition,
    TableDefinition,
} from "../parser/TableDefinition.parser";
import { QueryPlanner } from "./QueryPlanner";

export type InsertRow = {
    rowId: number;
    row: any;
};

export class InsertManager {
    constructor(
        private readonly database: DatabaseFile,
        private readonly insert: stmt_insert
    ) {}

    static findDefaultConstraint(
        column: TableColumnDefinition
    ): column_constraint_default | undefined {
        for (const constraint of column.constraints) {
            if (
                constraint?.column_constraint_core.kind ===
                ASTKinds.column_constraint_default
            ) {
                return undefined;
            }
        }
    }

    static parseLiteral(
        value: value_literal
    ): number | string | boolean | null {
        switch (value.kind) {
            case ASTKinds.quoted_string:
                return value.value;
            case ASTKinds.literal_null:
                return null;
            case ASTKinds.literal_false:
                return false;
            case ASTKinds.literal_true:
                return true;
            case ASTKinds.num:
                return parseFloat(value.value);
        }

        if ("value" in value) {
            return value.value;
        }

        return null;
    }

    static constructDefaultRow(tableDefinition: TableDefinition): any {
        return tableDefinition.columns.reduce((state, column) => {
            const defaultConstraint =
                InsertManager.findDefaultConstraint(column);

            return {
                ...state,
                [column.name]:
                    defaultConstraint?.value.kind ===
                    ASTKinds.column_constraint_default_value_1
                        ? InsertManager.parseLiteral(
                              defaultConstraint.value.literal
                          )
                        : null,
            };
        }, {});
    }

    private static evaluateValueExpression(
        database: DatabaseFile,
        exp: expression_front_recursive
    ): any {
        // TODO: support performing table scans if needed.
        return QueryPlanner.evaluateExpressionOrAggregate(database, [], exp)
            .value;
    }

    static constructRowsFromValues(
        database: DatabaseFile,
        tableDefinition: TableDefinition,
        insert_core: stmt_insert_core_values,
        appliedColumnNames: identifier[]
    ): InsertRow[] {
        const rows = [
            insert_core.value_rows.row,
            ...insert_core.value_rows.other_rows.map((r) => r.row),
        ];
        const columns =
            appliedColumnNames.length > 0
                ? tableDefinition.columns.filter((column) =>
                      appliedColumnNames.some((ac) => ac.value === column.name)
                  )
                : tableDefinition.columns;
        return rows.map<InsertRow>((valueRow, idx) => {
            const defaultRow =
                InsertManager.constructDefaultRow(tableDefinition);
            const expressions = [
                valueRow.expressions.expression,
                ...valueRow.expressions.other_expressions.map(
                    (e) => e.expression
                ),
            ];

            if (columns.length !== expressions.length) {
                throw new Error(
                    `Specified ${columns.length} columns but ${expressions.length} values`
                );
            }

            const row = columns.reduce((row, column, idx) => {
                const valueExpression = expressions[idx];
                return {
                    ...row,
                    [column.name]: InsertManager.evaluateValueExpression(
                        database,
                        valueExpression
                    ),
                };
            }, defaultRow);

            return {
                rowId: idx,
                row,
            };
        });
    }

    static validateTableConstraints(
        database: DatabaseFile,
        tableDefinition: TableDefinition,
        insertRow: InsertRow
    ) {
        for (const constraint of tableDefinition.constraints) {
            const constraintErrorPrefix = constraint.constraint_name
                ? `CONSTRAINT ${constraint.constraint_name.name.value}: `
                : "";
            const core = constraint.table_constraint_core;
            switch (core.kind) {
                case ASTKinds.table_constraint_foreign_key: {
                    throw new Error(
                        "Unimplemented table foreign key constraint"
                    );
                }
                case ASTKinds.table_constraint_check: {
                    const result = QueryPlanner.evaluateExpression(
                        database,
                        insertRow.row,
                        core.expression
                    );
                    if (!result.value) {
                        throw new Error(
                            `${constraintErrorPrefix}Row is violation CHECK (${result.name}) constriant.`
                        );
                    }
                    break;
                }
                case ASTKinds.table_constraint_unique: {
                    const indexedColumns = [
                        core.indexed_columns.column,
                        ...core.indexed_columns.other_columns.map(
                            (c) => c.column
                        ),
                    ];
                    const insertedRowValues = indexedColumns.map(
                        (column) =>
                            QueryPlanner.evaluateExpression(
                                database,
                                insertRow.row,
                                column.indexed_column_core
                            ).value
                    );
                    const conflictingRows = database.getRows(
                        tableDefinition.tableName,
                        (row) => {
                            const rowValues = indexedColumns.map(
                                (column) =>
                                    QueryPlanner.evaluateExpression(
                                        database,
                                        row,
                                        column.indexed_column_core
                                    ).value
                            );

                            return _.isEqual(rowValues, insertedRowValues);
                        }
                    );
                    if (conflictingRows.length > 0) {
                        throw new Error(
                            `${constraintErrorPrefix}Row is violating ${
                                core.kind === ASTKinds.table_constraint_unique
                                    ? "UNIQUE"
                                    : "PRIMARY"
                            } KEY constriant.`
                        );
                    }
                    break;
                }
            }
        }
    }

    static validateColumnConstraints(
        database: DatabaseFile,
        tableDefinition: TableDefinition,
        insertRow: InsertRow,
        column: TableColumnDefinition
    ) {
        const value = insertRow.row[column.name];
        for (const constraint of column.constraints) {
            const constraintErrorPrefix = constraint.constraint_name
                ? `CONSTRAINT ${constraint.constraint_name.name.value}: `
                : "";
            switch (constraint.column_constraint_core.kind) {
                case ASTKinds.column_constraint_check: {
                    const result = QueryPlanner.evaluateExpression(
                        database,
                        insertRow.row,
                        constraint.column_constraint_core.expression
                    );
                    if (!result.value) {
                        throw new Error(
                            `${constraintErrorPrefix}Column ${column.name} is violating CHECK (${result.name}) constriant.`
                        );
                    }
                    break;
                }
                case ASTKinds.column_constriant_primary_key: {
                    const conflictingRows = database.getRows(
                        tableDefinition.tableName,
                        (row) => row[column.name] === value
                    );
                    if (conflictingRows.length > 0) {
                        throw new Error(
                            `${constraintErrorPrefix}Column ${column.name} is violating PRIMARY KEY constriant.`
                        );
                    }
                    break;
                }
                case ASTKinds.column_constraint_unique: {
                    const conflictingRows = database.getRows(
                        tableDefinition.tableName,
                        (row) => row[column.name] === value
                    );
                    if (conflictingRows.length > 0) {
                        throw new Error(
                            `${constraintErrorPrefix}Column ${column.name} is violating UNIQUE constriant.`
                        );
                    }
                    break;
                }
                case ASTKinds.column_constraint_not_null:
                    if (_.isNil(value)) {
                        throw new Error(
                            `${constraintErrorPrefix}Column ${column.name} is NULL, violating NOT NULL constraint.`
                        );
                    }
            }
        }
    }

    static validateConstraints(
        database: DatabaseFile,
        tableDefinition: TableDefinition,
        insertRow: InsertRow
    ) {
        for (const column of tableDefinition.columns) {
            InsertManager.validateColumnConstraints(
                database,
                tableDefinition,
                insertRow,
                column
            );
        }

        InsertManager.validateTableConstraints(
            database,
            tableDefinition,
            insertRow
        );
    }

    static constructRows(
        database: DatabaseFile,
        tableDefinition: TableDefinition,
        insert_core: stmt_insert_core,
        appliedColumnNames: identifier[]
    ): InsertRow[] {
        switch (insert_core.kind) {
            case ASTKinds.stmt_insert_core_values:
                return InsertManager.constructRowsFromValues(
                    database,
                    tableDefinition,
                    insert_core,
                    appliedColumnNames
                );
            case ASTKinds.stmt_insert_core_stmt:
                // TODO: support "AS STMT" statements.
                return [];
            case ASTKinds.stmt_insert_core_default:
                return [
                    {
                        rowId: 0,
                        row: InsertManager.constructDefaultRow(tableDefinition),
                    },
                ];
        }
    }

    execute() {
        const {
            table,
            columns: rawColumns,
            insert_core,
            as_alias,
        } = this.insert;

        const columns = rawColumns
            ? [
                  rawColumns.columns.column,
                  ...rawColumns.columns.other_columns.map((c) => c.column),
              ]
            : [];
        const tableDefinition = this.database.schema.find(
            (s) => s.name === table.table_name.value
        )?.tableDefinition;

        if (!tableDefinition) {
            throw new Error("Failed to find table definition");
        }

        return InsertManager.constructRows(
            this.database,
            tableDefinition,
            this.insert.insert_core,
            columns
        );
    }
}
