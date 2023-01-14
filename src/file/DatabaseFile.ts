import _ from "lodash";
import { DatabaseHeader } from "./DatabaseFile.types";
import { DatabaseFileBTreePageUtil } from "./DatabaseFileBTreePage";
import { DatabaseFileHeaderUtil } from "./DatabaseFileHeader";
import { File } from "./File";

export class DatabaseFile extends File {
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
                return result;
            }
        );

        return { header, pages };
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
