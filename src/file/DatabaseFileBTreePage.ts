import _ from "lodash";
import {
    BTreeHeader,
    BTreeHeaderCommon,
    BTreeIndexData,
    BTreeIndexInteriorData,
    BTreePage,
    BTreePageOfType,
    BTreePageType,
    BTreeRecord,
    BTreeRow,
    BTreeTablePagePointer,
    DatabaseHeader,
} from "./DatabaseFile.types";

type RequestAdditionalPageFunction = (pageNumber: number) => Buffer;

export class DatabaseFileBTreePageUtil {
    static parseRecord(bytes: Buffer, dbHeader: DatabaseHeader): BTreeRecord[] {
        if (bytes.length === 0) {
            return [];
        }

        const records: BTreeRecord[] = [];

        let currentIndex = 0;
        const { value: headerSize, length: headerSizeLength } = this.readVarInt(
            bytes,
            currentIndex
        );
        currentIndex += headerSizeLength;

        const columnSerialTypes: number[] = [];
        while (currentIndex < headerSize) {
            const { value: serialType, length: serialTypeLength } =
                this.readVarInt(bytes, currentIndex);

            currentIndex += serialTypeLength;

            columnSerialTypes.push(serialType);
        }

        for (const serialType of columnSerialTypes) {
            if (serialType === 0) {
                records.push({
                    type: "NULL",
                    value: null,
                });
            }

            if (serialType === 1) {
                records.push({
                    type: "int_8",
                    value: bytes.readInt8(currentIndex),
                });

                currentIndex++;
            }

            if (serialType === 2) {
                records.push({
                    type: "int_16",
                    value: bytes.readInt16BE(currentIndex),
                });

                currentIndex += 2;
            }

            if (serialType === 3) {
                records.push({
                    type: "int_24",
                    value: bytes.readIntBE(currentIndex, 3),
                });

                currentIndex += 3;
            }

            if (serialType === 4) {
                records.push({
                    type: "int_32",
                    value: bytes.readInt32BE(currentIndex),
                });

                currentIndex += 4;
            }

            if (serialType === 5) {
                records.push({
                    type: "int_48",
                    value: bytes.readIntBE(currentIndex, 6),
                });

                currentIndex += 6;
            }

            if (serialType === 6) {
                records.push({
                    type: "int_64",
                    value: Number(bytes.readBigInt64BE(currentIndex)),
                });

                currentIndex += 8;
            }

            if (serialType === 7) {
                records.push({
                    type: "float_64",
                    value: bytes.readDoubleBE(currentIndex),
                });

                currentIndex += 8;
            }

            if (serialType === 8) {
                records.push({
                    type: "0",
                    value: 0,
                });
            }

            if (serialType === 9) {
                records.push({
                    type: "1",
                    value: 1,
                });
            }

            if (serialType === 10 || serialType === 11) {
                records.push({
                    type: "reserved",
                    value: null,
                });
            }

            if (serialType >= 12 && serialType % 2 === 0) {
                const blobLength = (serialType - 12) / 2;
                records.push({
                    type: "blob",
                    value: [
                        ...bytes.subarray(
                            currentIndex,
                            currentIndex + blobLength
                        ),
                    ],
                });

                currentIndex += blobLength;
            }

            if (serialType >= 13 && serialType % 2 === 1) {
                const textLength = (serialType - 13) / 2;
                const textBuffer = bytes.subarray(
                    currentIndex,
                    currentIndex + textLength
                );

                // TODO: support utf-16be.
                const value =
                    dbHeader.textEncoding === "utf16le" ||
                    dbHeader.textEncoding === "utf8"
                        ? textBuffer.toString(dbHeader.textEncoding)
                        : textBuffer.toString("utf-8");

                records.push({
                    type: "text",
                    value,
                });

                currentIndex += textLength;
            }
        }

        return records;
    }

    // Returns undefined if the provided page is not a BTree, typically meaning it's an overflow page.
    static parsePageType(bytes: Buffer): BTreePageType | undefined {
        const value = bytes.readUInt8(0);

        if (value === 2) {
            return "index_interior";
        }

        if (value === 5) {
            return "table_interior";
        }

        if (value === 10) {
            return "index_leaf";
        }

        if (value === 13) {
            return "table_leaf";
        }

        return undefined;
    }

    // Returns undefined if the provided page is not a BTree, typically meaning it's an overflow page.
    static parseHeader(
        bytes: Buffer,
        pageNumber: number
    ): BTreeHeader | undefined {
        const pageType = this.parsePageType(bytes);

        if (!pageType) {
            return undefined;
        }

        const startIdx = bytes.readUInt16BE(5);
        const commonData: BTreeHeaderCommon = {
            pageNumber,
            firstFreeblockIndex: bytes.readUInt16BE(1),
            numberCells: bytes.readUInt16BE(3),
            cellContentStartIndex: startIdx === 0 ? 65536 : startIdx,
            numberFragmentedFreeBytesInCellContent: bytes.readUInt8(7),
        };

        if (pageType === "table_interior" || pageType === "index_interior") {
            return {
                type: pageType,
                ...commonData,
                rightmostPointer: bytes.readUInt32BE(8),
            };
        }

        return {
            type: pageType,
            ...commonData,
        };
    }

    // Will be in the range of 0...(2^64) - 1
    static readVarInt(
        bytes: Buffer,
        idx: number
    ): { value: number; length: number } {
        const candidateBytes = bytes.subarray(idx, idx + 9);

        // A byte will have a 1 in the high bit if there are no more bytes following it.
        // Thus, we find the first high bit equal to 1, and then can add the bytes.
        // There is only one complication: if we are using every byte, we use all 8 bits of the last byte.
        const firstHighBitByteIndexCandidate = candidateBytes.findIndex(
            (b) => (b & (1 << 7)) === 0
        );
        const firstHighBitByteIndex =
            firstHighBitByteIndexCandidate === -1
                ? 8
                : firstHighBitByteIndexCandidate;

        const value = candidateBytes.reduce((state, byte, idx) => {
            const numBits = idx === 8 ? 8 : 7;
            const mask = numBits === 8 ? 0b1111_1111 : 0b0111_1111;

            if (idx > firstHighBitByteIndex) {
                return state;
            }

            return (state << numBits) | (byte & mask);
        }, 0);

        return { value, length: firstHighBitByteIndex + 1 };
    }

    static getCellOffsets(bytes: Buffer, pageHeader: BTreeHeader): number[] {
        const startOffset =
            (pageHeader.pageNumber === 0 ? 100 : 0) +
            (pageHeader.type === "table_interior" ||
            pageHeader.type === "index_interior"
                ? 12
                : 8);
        const cellPointerBytes = bytes.subarray(
            startOffset,
            startOffset + pageHeader.numberCells * 2
        );

        return _.range(0, pageHeader.numberCells).map((idx) =>
            cellPointerBytes.readUInt16BE(idx * 2)
        );
    }

    static parseBTreeIndexInterior(
        bytes: Buffer,
        dbHeader: DatabaseHeader,
        pageHeader: BTreeHeader,
        requestAdditionalPage: RequestAdditionalPageFunction,
        columns: string[]
    ): BTreePageOfType<"index_interior"> {
        if (pageHeader.type !== "index_interior") {
            throw new Error("Page is not an index interior");
        }

        const offsets = this.getCellOffsets(bytes, pageHeader);
        const indices = offsets.map<BTreeIndexInteriorData>((offset) => {
            let currentIndex = offset;
            const pageNumber = bytes.readUInt32BE(currentIndex);
            currentIndex += 4;

            const { value: payloadSize, length: payloadSizeLength } =
                this.readVarInt(bytes, currentIndex);
            currentIndex += payloadSizeLength;

            const usableSize =
                dbHeader.pageSizeBytes - dbHeader.unusedReservePageSpace;
            const maxPayload = usableSize - 35;
            const minPayload = ((usableSize - 12) * 32) / 255 - 23;
            const K =
                minPayload + ((payloadSize - minPayload) % (usableSize - 4));
            const storedSize = Math.floor(
                payloadSize <= maxPayload
                    ? payloadSize
                    : payloadSize > maxPayload && K <= maxPayload
                    ? K
                    : minPayload
            );

            const storedData = bytes.subarray(
                currentIndex,
                currentIndex + storedSize
            );

            currentIndex += storedSize;

            const hasOverflow = storedSize < payloadSize;
            const overflowPage = hasOverflow
                ? bytes.readUInt32BE(currentIndex)
                : undefined;
            if (hasOverflow) {
                currentIndex += 4;
            }

            const { buffer: overflowPageData, otherOverflowPages } =
                overflowPage
                    ? this.readOverflowRecordData(
                          dbHeader,
                          overflowPage,
                          requestAdditionalPage
                      )
                    : { buffer: undefined, otherOverflowPages: [] };
            const data = overflowPageData
                ? Buffer.from([...storedData, ...overflowPageData])
                : storedData;

            const records = this.parseRecord(data, dbHeader);
            return {
                payloadSize,
                storedSize,
                pageNumber,
                overflowPage,
                records,
                otherOverflowPages,
                pageOffset: offset,
                cells: DatabaseFileBTreePageUtil.zipRecordsAndColumns(
                    records,
                    columns
                ),
            };
        });

        return {
            indices: [
                ...indices,
                {
                    storedSize: 0,
                    payloadSize: 0,
                    records: [],
                    pageNumber: pageHeader.rightmostPointer,
                    overflowPage: undefined,
                    otherOverflowPages: [],
                    cells: {},
                    pageOffset: 8,
                },
            ],
            header: pageHeader,
            type: "index_interior",
        };
    }

    static parseBTreeIndexLeaf(
        bytes: Buffer,
        dbHeader: DatabaseHeader,
        pageHeader: BTreeHeader,
        requestAdditionalPage: RequestAdditionalPageFunction,
        columns: string[]
    ): BTreePageOfType<"index_leaf"> {
        if (pageHeader.type !== "index_leaf") {
            throw new Error("Page is not an index leaf");
        }

        const offsets = this.getCellOffsets(bytes, pageHeader);
        const indices = offsets.map<BTreeIndexData>((offset) => {
            let currentIndex = offset;
            const { value: payloadSize, length: payloadSizeLength } =
                this.readVarInt(bytes, currentIndex);
            currentIndex += payloadSizeLength;

            const usableSize =
                dbHeader.pageSizeBytes - dbHeader.unusedReservePageSpace;
            const maxPayload = ((usableSize - 12) * 64) / 255 - 23;
            const minPayload = ((usableSize - 12) * 32) / 255 - 23;
            const K =
                minPayload + ((payloadSize - minPayload) % (usableSize - 4));
            const storedSize = Math.floor(
                payloadSize <= maxPayload
                    ? payloadSize
                    : payloadSize > maxPayload && K <= maxPayload
                    ? K
                    : minPayload
            );

            const storedData = bytes.subarray(
                currentIndex,
                currentIndex + storedSize
            );

            currentIndex += storedSize;

            const hasOverflow = storedSize < payloadSize;
            const overflowPage = hasOverflow
                ? bytes.readUInt32BE(currentIndex)
                : undefined;

            const { buffer: overflowPageData, otherOverflowPages } =
                overflowPage
                    ? this.readOverflowRecordData(
                          dbHeader,
                          overflowPage,
                          requestAdditionalPage
                      )
                    : { buffer: undefined, otherOverflowPages: [] };
            const data = overflowPageData
                ? Buffer.from([...storedData, ...overflowPageData])
                : storedData;

            const records = this.parseRecord(data, dbHeader);
            return {
                payloadSize,
                storedSize,
                overflowPage,
                records,
                otherOverflowPages,
                pageOffset: offset,
                cells: DatabaseFileBTreePageUtil.zipRecordsAndColumns(
                    records,
                    columns
                ),
            };
        });

        return {
            type: "index_leaf",
            indices,
            header: pageHeader,
        };
    }

    static readOverflowRecordData(
        dbHeader: DatabaseHeader,
        overflowPage: number,
        requestAdditionalPage: RequestAdditionalPageFunction
    ): { buffer: Buffer; otherOverflowPages: number[] } {
        let currentPageNumber = overflowPage;
        const otherOverflowPages: number[] = [];
        let fullBuffer: Buffer = Buffer.alloc(0);

        // We read the current page until we reach a 0 page, which indicates we have read all of the data.
        while (currentPageNumber > 0) {
            const page = requestAdditionalPage(currentPageNumber);
            const nextPageNumber = page.readInt32BE(0);

            const contents = page.subarray(4);
            fullBuffer = Buffer.from([...fullBuffer, ...contents]);

            // If the next page number is 0, our loop will end.
            currentPageNumber = nextPageNumber;

            if (nextPageNumber !== 0) {
                otherOverflowPages.push(nextPageNumber);
            }
        }

        return { buffer: fullBuffer, otherOverflowPages };
    }

    static parseBTreeTableLeaf(
        bytes: Buffer,
        dbHeader: DatabaseHeader,
        pageHeader: BTreeHeader,
        requestAdditionalPage: RequestAdditionalPageFunction,
        columns: string[]
    ): BTreePageOfType<"table_leaf"> {
        if (pageHeader.type !== "table_leaf") {
            throw new Error("Page is not a table leaf");
        }

        const offsets = this.getCellOffsets(bytes, pageHeader);
        const rows = offsets.map<BTreeRow>((offset) => {
            let currentIndex = offset;
            const { value: payloadSize, length: payloadSizeLength } =
                this.readVarInt(bytes, currentIndex);

            currentIndex += payloadSizeLength;

            const { value: rowId, length: rowIdLength } = this.readVarInt(
                bytes,
                currentIndex
            );
            currentIndex += rowIdLength;

            const usableSize =
                dbHeader.pageSizeBytes - dbHeader.unusedReservePageSpace;
            const maxPayload = usableSize - 35;
            const minPayload = ((usableSize - 12) * 32) / 255 - 23;
            const K =
                minPayload + ((payloadSize - minPayload) % (usableSize - 4));
            const storedSize = Math.floor(
                payloadSize <= maxPayload
                    ? payloadSize
                    : payloadSize > maxPayload && K <= maxPayload
                    ? K
                    : minPayload
            );

            const storedData = bytes.subarray(
                currentIndex,
                currentIndex + storedSize
            );

            currentIndex += storedSize;

            const hasOverflow = storedSize < payloadSize;
            const overflowPage = hasOverflow
                ? bytes.readUInt32BE(currentIndex)
                : undefined;
            if (hasOverflow) {
                currentIndex += 4;
            }

            const { buffer: overflowPageData, otherOverflowPages } =
                overflowPage
                    ? this.readOverflowRecordData(
                          dbHeader,
                          overflowPage,
                          requestAdditionalPage
                      )
                    : { buffer: null, otherOverflowPages: [] };
            const data = overflowPageData
                ? Buffer.from([...storedData, ...overflowPageData])
                : storedData;

            const records = this.parseRecord(data, dbHeader);
            return {
                payloadSize,
                storedSize,
                rowId,
                overflowPage,
                records,
                otherOverflowPages,
                pageOffset: offset,
                cells: DatabaseFileBTreePageUtil.zipRecordsAndColumns(
                    records,
                    columns
                ),
            };
        });

        return { type: "table_leaf", rows, header: pageHeader };
    }

    static zipRecordsAndColumns(
        records: BTreeRecord[],
        columns: string[]
    ): any {
        return records.reduce((state, cell, idx) => {
            const column = columns[idx];

            return {
                ...state,
                [column ?? `unknown_${idx}`]: cell.value,
            };
        }, {});
    }

    static parseBTreeTableInterior(
        bytes: Buffer,
        dbHeader: DatabaseHeader,
        pageHeader: BTreeHeader
    ): BTreePageOfType<"table_interior"> {
        if (pageHeader.type !== "table_interior") {
            throw new Error("Not an interior page");
        }

        const offsets = this.getCellOffsets(bytes, pageHeader);
        const pointers = offsets.map<BTreeTablePagePointer>((offset) => {
            let currentIndex = offset;
            const pageNumber = bytes.readUInt32BE(currentIndex);
            currentIndex += 4;
            return {
                pageNumber,
                key: this.readVarInt(bytes, currentIndex).value,
                pageOffset: offset,
            };
        });

        return {
            type: "table_interior",
            pointers: [
                ...pointers,
                {
                    key: 0,
                    pageNumber: pageHeader.rightmostPointer,
                    pageOffset: -1,
                },
            ],
            header: pageHeader,
        };
    }

    static parseBTreePage(
        bytes: Buffer,
        pageNumber: number,
        dbHeader: DatabaseHeader,
        requestAdditionalPage: RequestAdditionalPageFunction,
        columns: string[]
    ): BTreePage | undefined {
        try {
            const bytesWithoutPageZeroHeader =
                pageNumber === 0 ? bytes.subarray(100) : bytes;
            const header = this.parseHeader(
                bytesWithoutPageZeroHeader,
                pageNumber
            );

            if (!header) {
                return undefined;
            }

            switch (header.type) {
                case "table_interior":
                    return this.parseBTreeTableInterior(
                        bytes,
                        dbHeader,
                        header
                    );
                case "table_leaf":
                    return this.parseBTreeTableLeaf(
                        bytes,
                        dbHeader,
                        header,
                        requestAdditionalPage,
                        columns
                    );
                case "index_leaf":
                    return this.parseBTreeIndexLeaf(
                        bytes,
                        dbHeader,
                        header,
                        requestAdditionalPage,
                        columns
                    );
                case "index_interior":
                    return this.parseBTreeIndexInterior(
                        bytes,
                        dbHeader,
                        header,
                        requestAdditionalPage,
                        columns
                    );
            }
        } catch (err) {
            console.error(`Error on page ${pageNumber}`, err);
            return undefined;
        }
    }
}
