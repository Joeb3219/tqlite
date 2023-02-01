import {
    IndexDefinition,
    IndexDefinitionParser,
} from "../parser/IndexDefinition.parser";
import {
    TableDefinition,
    TableDefinitionParser,
} from "../parser/TableDefinition.parser";
import { BTreePage, DatabaseHeader } from "./DatabaseFile.types";
import { DatabaseFileBTreePageUtil } from "./DatabaseFileBTreePage";
import { DatabaseFileHeaderUtil } from "./DatabaseFileHeader";
import { File } from "./File";

type Row = (string | number | boolean | null | number[])[];

export type MasterSchemaEntry = {
    type: "table" | "index" | "unknown";
    name: string;
    tbl_name: string;
    rootpage: number;
    sql?: string;
    tableDefinition?: TableDefinition;
    indexDefinition?: IndexDefinition;
};

export type MasterSchema = MasterSchemaEntry[];

export class DatabaseFile extends File {
    header: DatabaseHeader;
    schema: MasterSchemaEntry[];

    constructor(bytes: ArrayBuffer | Buffer) {
        super(bytes);

        this.header = DatabaseFileHeaderUtil.parseHeader(this);
        this.schema = this.readMasterSchema();
    }

    // TODO: implement where for indices
    getTableRowsInternal(
        pageNumber: number,
        columns: string[],
        evaluationCriterion: (row: any) => boolean
    ): any[] {
        const rootPage = this.loadPage(pageNumber, columns);

        if (rootPage?.type === "table_interior") {
            return rootPage.pointers.flatMap((p) =>
                this.getTableRowsInternal(p.pageNumber, columns, evaluationCriterion)
            );
        }

        if (rootPage?.type === "index_leaf") {
            return rootPage.indices.map((i) => i.cells);
        }

        if (rootPage?.type === "index_interior") {
            return rootPage.indices.flatMap((p) =>
                this.getTableRowsInternal(p.pageNumber, columns, evaluationCriterion)
            );
        }

        return rootPage.rows.map<Row>((recordRow) => {
            return recordRow.cells;
        }).filter(r => evaluationCriterion(r));
    }

    getRows(tableOrIndexName: string, evaluationCriterion: (row: any) => boolean): any[] {
        const entry = this.schema.find((s) => s.name === tableOrIndexName);

        if (!entry?.rootpage) {
            return [];
        }

        const columns: string[] =
            entry.tableDefinition?.columns?.map((c) => c.name) ??
            entry.indexDefinition?.columns ??
            [];
        return this.getTableRowsInternal(entry.rootpage, columns, evaluationCriterion);
    }

    parseTableDefinition(sql?: string): TableDefinition | undefined {
        if (!sql) {
            return undefined;
        }

        try {
            const tableParser = new TableDefinitionParser(sql);
            return tableParser.tableDefinition;
        } catch (err) {
            console.warn("Failed to parse table definition", err);
            return undefined;
        }
    }

    parseIndexDefinition(sql?: string): IndexDefinition | undefined {
        if (!sql) {
            return undefined;
        }

        try {
            const tableParser = new IndexDefinitionParser(sql);
            return tableParser.indexDefinition;
        } catch (err) {
            console.warn("Failed to parse index definition", err);
            return undefined;
        }
    }

    findMasterSchemaPages(): BTreePage[] {
        const rootPage = this.loadPage(1, []);
        if (
            rootPage?.type === "table_leaf" ||
            rootPage?.type === "index_leaf" ||
            rootPage?.type === "index_interior"
        ) {
            return [rootPage];
        }

        return (rootPage?.pointers ?? []).map((p) =>
            this.loadPage(p.pageNumber, [])
        );
    }

    readMasterSchema(): MasterSchemaEntry[] {
        const pages = this.findMasterSchemaPages();
        console.log(pages);

        return pages.flatMap((page) => {
            if (page.type !== "table_leaf") {
                return [];
            }
            return page.rows.map<MasterSchemaEntry>((row) => {
                const [
                    typeRecord,
                    nameRecord,
                    tableNameRecord,
                    rootPageRecord,
                    sqlRecord,
                ] = row.records;

                console.log(row);
                if (typeRecord.type !== "text") {
                    throw new Error(
                        `Expected 'type' to be of type 'text', but found ${typeRecord.type}`
                    );
                }

                if (nameRecord.type !== "text") {
                    throw new Error(
                        `Expected 'name' to be of type 'text', but found ${nameRecord.type}`
                    );
                }

                if (tableNameRecord.type !== "text") {
                    throw new Error(
                        `Expected 'tbl_name' to be of type 'text', but found ${tableNameRecord.type}`
                    );
                }

                if (rootPageRecord.type !== "int_8") {
                    throw new Error(
                        `Expected 'rootpage' to be of type 'int', but found ${rootPageRecord.type}`
                    );
                }

                if (
                    sqlRecord &&
                    sqlRecord.type !== "text" &&
                    sqlRecord.type !== "NULL"
                ) {
                    throw new Error(
                        `Expected 'sql' to be of type 'text' or 'NULL', but found ${sqlRecord.type}`
                    );
                }

                const type: MasterSchemaEntry["type"] =
                    typeRecord.value === "table"
                        ? "table"
                        : typeRecord.value === "index"
                        ? "index"
                        : "unknown";
                const sql =
                    sqlRecord?.type === "text" ? sqlRecord.value : undefined;
                return {
                    type,
                    sql,
                    name: nameRecord.value,
                    rootpage: rootPageRecord.value,
                    tbl_name: tableNameRecord.value,
                    tableDefinition:
                        type === "table"
                            ? this.parseTableDefinition(sql)
                            : undefined,
                    indexDefinition:
                        type === "index"
                            ? this.parseIndexDefinition(sql) ?? {
                                  unique: false,
                                  tableName: tableNameRecord.value,
                                  indexName: nameRecord.value,
                                  columns: ["key", "id"],
                              }
                            : undefined,
                };
            });
        });
    }

    getBytesOnPage(header: DatabaseHeader, pageNumber: number): Buffer {
        // If all pages are of size N, we can easily compute the starting address.
        const realStartIdx = header.pageSizeBytes * pageNumber;

        // We aren't always going to return `header.pageSizeBytes` bytes.
        // This is fine.
        return this.data.subarray(
            realStartIdx,
            realStartIdx + header.pageSizeBytes
        );
    }

    loadPage(pageNumber: number, columns: string[]): BTreePage {
        const data = this.getBytesOnPage(this.header, pageNumber - 1);
        const result = DatabaseFileBTreePageUtil.parseBTreePage(
            data,
            pageNumber - 1,
            this.header,
            (pageNumber) => this.getBytesOnPage(this.header, pageNumber - 1),
            columns
        );

        if (!result) {
            throw new Error(`Failed to read page ${pageNumber}`);
        }

        return result;
    }
}
