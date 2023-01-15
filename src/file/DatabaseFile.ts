import _ from "lodash";
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
    rows: Row[];
};

export type MasterSchema = MasterSchemaEntry[];

export class DatabaseFile extends File {
    pages: Record<number, BTreePage> = {};

    getTableRows(pageNumber: number): Row[] {
        const page = this.pages[pageNumber - 1];

        if (page?.type !== "table_leaf") {
            console.log("encountered non leaf", { pageNumber, page });
            return [];
        }

        return page.rows.map<Row>((recordRow) => {
            return recordRow.records.map((r) => r.value);
        });
    }

    findMasterSchemaPage(): BTreePage | undefined {
        if (this.pages[0]?.type === "table_leaf") {
            return this.pages[0];
        }

        const redirect = this.pages[0].pointers[0]?.pageNumber;
        return this.pages[redirect];
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
                sql: sqlRecord?.type === "text" ? sqlRecord.value : undefined,
                rows:
                    rootPageRecord.value !== 0
                        ? this.getTableRows(rootPageRecord.value)
                        : [],
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
        return masterSchema;
    }

    loadPage(header: DatabaseHeader, pageNumber: number) {
        // If all pages are of size N, we can easily compute the starting address.
        const realStartIdx = header.pageSizeBytes * pageNumber;

        // We aren't always going to return `header.pageSizeBytes` bytes.
        // This is fine.
        const data = this.data.subarray(
            realStartIdx,
            realStartIdx + header.pageSizeBytes
        );

        const result = DatabaseFileBTreePageUtil.parseBTreePage(
            data,
            pageNumber,
            header
        );

        if (!result) {
            console.error(`Failed to parse page ${pageNumber}`);
            return;
        }

        this.pages[pageNumber] = result;
    }
}
