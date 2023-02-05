import {
    ASTKinds,
    column_constraint,
    parse,
    stmt_create_table_core,
    stmt_create_table_core_column_definition,
    table_constraint,
} from "../parser-autogen/parser";
import { ParserUtil } from "./Parser.util";

export type ColumnAffinity = "text" | "numeric" | "integer" | "real" | "blob";

export type TableColumnDefinition = {
    name: string;
    type?: string;
    affinity: ColumnAffinity;
    constraints: column_constraint[];
};

export type TableDefinition = {
    schemaName?: string;
    tableName: string;
    temporary: boolean;
    // Internal tables have names that start with sqlite_
    isInternal: boolean;
    columns: TableColumnDefinition[];
    constraints: table_constraint[];
    createIfNotExists: boolean;
};

export class TableDefinitionParser {
    constructor(private readonly sql: string) {}

    // See section 3.1, "Determination Of Column Affinity" of https://www.sqlite.org/datatype3.html
    static typeToAffinity(type?: string): ColumnAffinity {
        const normalizedType = type?.toLowerCase();

        // Rule 1 -- if it contains "int", it must be an INTEGER.
        if (normalizedType?.includes("int")) {
            return "integer";
        }

        // Rule 2 -- "char" "clob" and "text" are TEXT.
        if (
            ["char", "clob", "text"].some((candidate) =>
                normalizedType?.includes(candidate)
            )
        ) {
            return "text";
        }

        // Rule 3 -- "blob" and undefined types are BLOB.
        if (normalizedType?.includes("blob") || !normalizedType) {
            return "blob";
        }

        // Rule 4 -- "doub" "floa" and "real" are REAL.
        if (
            ["doub", "floa", "real"].some((candidate) =>
                normalizedType.includes(candidate)
            )
        ) {
            return "real";
        }

        // Rule 5 -- we default to NUMERIC.
        return "numeric";
    }

    static processColumn(
        columnDef: stmt_create_table_core_column_definition
    ): TableColumnDefinition {
        const { name, type, constraints } = columnDef;

        const columnType = type?.name.value;

        return {
            constraints,
            name: name.value,
            type: columnType,
            affinity: TableDefinitionParser.typeToAffinity(columnType),
        };
    }

    static processColumnsAndConstraints(core: stmt_create_table_core): {
        columns: TableColumnDefinition[];
        constraints: table_constraint[];
    } {
        if (core.kind === ASTKinds.stmt_create_table_core_as) {
            // TODO: support `create table as`
            return {
                columns: [],
                constraints: [],
            };
        }

        const elements = [
            core.definitions.element,
            ...core.definitions.other_elements.map((e) => e.element),
        ];
        return elements.reduce<{
            columns: TableColumnDefinition[];
            constraints: table_constraint[];
        }>(
            (state, element) => {
                if (element.kind === ASTKinds.table_constraint) {
                    return {
                        columns: state.columns,
                        constraints: [...state.constraints, element],
                    };
                } else {
                    return {
                        columns: [
                            ...state.columns,
                            TableDefinitionParser.processColumn(element),
                        ],
                        constraints: state.constraints,
                    };
                }
            },
            {
                columns: [],
                constraints: [],
            }
        );
    }

    execute(): TableDefinition | undefined {
        const parsed = parse(this.sql);
        if (parsed.errs.length || !parsed.ast) {
            ParserUtil.reportParseError(this.sql, parsed);
            return;
        }

        const stmts = [
            parsed.ast.stmt_list.stmt,
            ...parsed.ast.stmt_list.other_stmts,
        ];
        const firstStatement = stmts[0];

        if (stmts.length !== 1 || !firstStatement) {
            throw new Error(
                `Expected to find 1 statement, but found ${stmts.length}`
            );
        }

        if (firstStatement.kind !== ASTKinds.stmt_create_table) {
            throw new Error(
                `Expected create table declaration, but found ${firstStatement.kind}`
            );
        }

        const { name, temporary, create_table_core, if_not_exists } =
            firstStatement;
        const tableName = name.table_name.value;
        const { columns, constraints } =
            TableDefinitionParser.processColumnsAndConstraints(
                create_table_core
            );

        return {
            tableName,
            columns,
            constraints,
            schemaName: name.schema_name?.name.value,
            isInternal: name.table_name.value.startsWith("sqlite_"),
            temporary: !!temporary,
            createIfNotExists: !!if_not_exists,
        };
    }
}
