import _ from "lodash";
import { BTreePageOfType, DatabaseHeader } from "./DatabaseFile.types";
import { DatabaseFileBTreePageUtil } from "./DatabaseFileBTreePage";
import { DatabaseFileHeaderUtil } from "./DatabaseFileHeader";
import { File } from "./File";

export type MasterSchemaEntry = {
    type: "table" | "index" | "unknown";
    name: string;
    tbl_name: string;
    rootpage: number;
    sql?: string;
};

export type MasterSchema = MasterSchemaEntry[];

export class DatabaseFile extends File {
    static readMasterSchema(
        page: BTreePageOfType<"table_leaf">
    ): MasterSchemaEntry[] {
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
            };
        });
    }

    readDatabase() {
        const header = DatabaseFileHeaderUtil.parseHeader(this);

        const pages = _.range(0, header.databaseFileSizeInPages).map(
            (pageIdx) => {
                const page = this.readPage(header, pageIdx);
                const result = DatabaseFileBTreePageUtil.parseBTreePage(
                    page,
                    pageIdx,
                    header
                );

                if (!result) {
                    return;
                }

                if (pageIdx === 21 && result.type === "table_leaf") {
                    const schema = DatabaseFile.readMasterSchema(result);
                    console.log(JSON.stringify(schema, null, 2));
                }

                return;
            }
        );

        return { header, pages: [] };
    }

    readPage(header: DatabaseHeader, pageNumber: number): Buffer {
        // If all pages are of size N, we can easily compute the starting address.
        // The first page contains the header that the page size and other info came from.
        // Readers won't want this data, so we just skip past it.
        const realStartIdx = header.pageSizeBytes * pageNumber;
        const modifiedStartIdx =
            pageNumber === 0 ? realStartIdx + 100 : realStartIdx;

        // We aren't always going to return `header.pageSizeBytes` bytes.
        // This is fine.
        return this.data.subarray(
            realStartIdx,
            realStartIdx + header.pageSizeBytes
        );
    }
}
