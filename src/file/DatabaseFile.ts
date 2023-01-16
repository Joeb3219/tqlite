import _ from "lodash";
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
    rows: Row[];
    zipped: any[];
};

export type MasterSchema = MasterSchemaEntry[];

export class DatabaseFile extends File {
    pages: Record<number, BTreePage> = {};
    schema: MasterSchemaEntry[] = [];

    getTableRows(pageNumber: number): Row[] {
        const page = this.pages[pageNumber - 1];

        if (page?.type === "table_interior") {
            return page.pointers.flatMap((p) =>
                this.getTableRows(p.pageNumber)
            );
        }

        if (page?.type !== "table_leaf") {
            return [];
        }

        return page.rows.map<Row>((recordRow) => {
            return recordRow.records.map((r) => r.value);
        });
    }

    parseTableDefinition(sql?: string): TableDefinition | undefined {
        if (!sql) {
            return undefined;
        }

        const tableParser = new TableDefinitionParser(sql);
        return tableParser.tableDefinition;
    }

    findMasterSchemaPage(): BTreePage | undefined {
        if (
            this.pages[0]?.type === "table_leaf" ||
            this.pages[0]?.type === "index_leaf"
        ) {
            return this.pages[0];
        }

        const redirect = this.pages[0]?.pointers[0]?.pageNumber;
        return this.pages[redirect - 1];
    }

    readMasterSchema(): MasterSchemaEntry[] {
        const page = this.findMasterSchemaPage();

        if (page?.type !== "table_leaf") {
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

            const sql =
                sqlRecord?.type === "text" ? sqlRecord.value : undefined;
            const rows =
                rootPageRecord.value !== 0
                    ? this.getTableRows(rootPageRecord.value)
                    : [];
            const tableDefinition = this.parseTableDefinition(sql);
            return {
                name: nameRecord.value,
                rootpage: rootPageRecord.value,
                tbl_name: tableNameRecord.value,
                type:
                    typeRecord.value === "table"
                        ? "table"
                        : typeRecord.value === "index"
                        ? "index"
                        : "unknown",
                sql,
                rows,
                tableDefinition,
                zipped: rows.map((row) =>
                    row.reduce((state, cell, idx) => {
                        const column = tableDefinition?.columns[idx];

                        return {
                            ...state,
                            [column?.name ?? `unknown_${idx}`]: cell,
                        };
                    }, {})
                ),
            };
        });
    }

    readDatabase() {
        const header = DatabaseFileHeaderUtil.parseHeader(this);

        // Load all pages into the class
        _.range(0, header.databaseFileSizeInPages).forEach((pageIdx) => {
            this.loadPage(header, pageIdx);
        });

        const masterSchema = this.readMasterSchema();
        return (this.schema = masterSchema);
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

    loadPage(header: DatabaseHeader, pageNumber: number) {
        const data = this.getBytesOnPage(header, pageNumber);
        const result = DatabaseFileBTreePageUtil.parseBTreePage(
            data,
            pageNumber,
            header,
            (pageNumber) => this.getBytesOnPage(header, pageNumber - 1)
        );

        if (!result) {
            console.info(`Page ${pageNumber} was not parsed as a BTree Page.`);
            return;
        }

        this.pages[pageNumber] = result;
    }
}
