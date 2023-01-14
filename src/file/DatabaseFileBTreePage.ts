import _ from "lodash";
import { DatabaseHeader } from "./DatabaseFile.types";

export type BTreeType =
    | "index_interior"
    | "index_leaf"
    | "table_interior"
    | "table_leaf";

type BTreeHeaderCommon = {
    firstFreeblockIndex: number;
    numberCells: number;
    cellContentStartIndex: number;
    numberFragmentedFreeBytesInCellContent: number;
    pageNumber: number;
};

export type BTreeHeader =
    | ({
          type: "index_interior" | "index_leaf" | "table_leaf";
      } & BTreeHeaderCommon)
    | ({
          type: "table_interior";
          rightmostPointer: number;
      } & BTreeHeaderCommon);

export type BTreePageType = BTreeHeader["type"];

export type BTreeRecord =
    | {
          type: "NULL";
          value: null;
      }
    | {
          type: "int_8";
          value: number;
      }
    | {
          type: "int_16";
          value: number;
      }
    | {
          type: "int_24";
          value: number;
      }
    | {
          type: "int_32";
          value: number;
      }
    | {
          type: "int_48";
          value: number;
      }
    | {
          type: "int_64";
          value: number;
      }
    | {
          type: "float_64";
          value: number;
      }
    | {
          type: "0";
          value: 0;
      }
    | {
          type: "1";
          value: 1;
      }
    | {
          type: "reserved";
          value: null;
      }
    | {
          type: "blob";
          value: number[];
      }
    | {
          type: "text";
          value: string;
      };

export type BTreeTablePagePointer = {
    key: number;
    pageNumber: number;
};

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
        const numColumns = headerSize - headerSizeLength;
        currentIndex += headerSizeLength;

        const columnSerialTypes: number[] = [];
        while (currentIndex < headerSize) {
            const { value: serialType, length: serialTypeLength } =
                this.readVarInt(bytes, currentIndex);
            currentIndex += serialTypeLength;

            columnSerialTypes.push(serialType);
        }

        console.log("there are " + columnSerialTypes.length + " columns", {
            columnSerialTypes,
        });

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

    static parsePageType(bytes: Buffer): BTreePageType {
        const value = bytes.readUint8(0);

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

        throw new Error(
            `Page type must be either 2, 5, 10, or 13, but found ${value}`
        );
    }

    static parseHeader(bytes: Buffer, pageNumber: number): BTreeHeader {
        const pageType = this.parsePageType(bytes);

        const startIdx = bytes.readUint16BE(5);
        const commonData: BTreeHeaderCommon = {
            pageNumber,
            firstFreeblockIndex: bytes.readUint16BE(1),
            numberCells: bytes.readUint16BE(3),
            cellContentStartIndex: startIdx === 0 ? 65536 : startIdx,
            numberFragmentedFreeBytesInCellContent: bytes.readUint8(7),
        };

        if (pageType === "table_interior") {
            return {
                type: "table_interior",
                ...commonData,
                rightmostPointer: bytes.readUInt32BE(8),
            };
        }

        return {
            type: pageType,
            ...commonData,
        };
    }

    // https://www.sqlite.org/src4/doc/trunk/www/varint.wiki
    // Will be in the range of 0...(2^64) - 1
    static readVarInt(
        bytes: Buffer,
        idx: number
    ): { value: number; length: number } {
        const candidateBytes = bytes.subarray(idx, idx + 9);
        const firstByte = candidateBytes.readUInt8(0);

        if (firstByte >= 0 && firstByte <= 240) {
            return { value: firstByte, length: 1 };
        }

        if (firstByte >= 241 && firstByte <= 248) {
            return {
                value:
                    240 + 256 * (firstByte - 241) + candidateBytes.readUInt8(1),
                length: 2,
            };
        }

        if (firstByte === 249) {
            return {
                value:
                    2288 +
                    256 * candidateBytes.readUInt8(1) +
                    candidateBytes.readUInt8(2),
                length: 4,
            };
        }

        const numBytes = firstByte - 250 + 3;
        return {
            value: _.range(0, numBytes).reduce((state, idx) => {
                return (state << 8) | candidateBytes.readUint8(idx);
            }, 0x00),
            length: numBytes + 1,
        };
    }

    static getCellOffsets(bytes: Buffer, pageHeader: BTreeHeader): number[] {
        const startOffset =
            (pageHeader.pageNumber === 0 ? 100 : 0) +
            (pageHeader.type === "table_interior" ? 12 : 8);
        const cellPointerBytes = bytes.subarray(
            startOffset,
            startOffset + pageHeader.numberCells * 2
        );
        return _.range(0, pageHeader.numberCells).map((idx) =>
            cellPointerBytes.readUint16BE(idx * 2)
        );
    }

    static parseBTreeTableLeaf(
        bytes: Buffer,
        dbHeader: DatabaseHeader,
        pageHeader: BTreeHeader
    ) {
        if (
            pageHeader.type !== "table_leaf" &&
            pageHeader.type !== "table_interior"
        ) {
            return undefined;
        }

        const offsets = this.getCellOffsets(bytes, pageHeader);
        const parsedCells = offsets.map((offset) => {
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

            const data = bytes.subarray(
                currentIndex,
                currentIndex + storedSize
            );
            currentIndex += storedSize; // - payloadSizeLength - rowIdLength;

            const hasOverflow = storedSize < payloadSize;
            const overflowPage = hasOverflow
                ? bytes.readUint32BE(currentIndex)
                : undefined;
            if (hasOverflow) {
                currentIndex += 4;
            }

            const cell = {
                payloadSize,
                rowId,
                data: data.toString("utf8"),
                overflowPage,
                records: this.parseRecord(data, dbHeader),
            };
            return cell;
        });

        return parsedCells;
    }

    static parseBTreeTableInterior(
        bytes: Buffer,
        dbHeader: DatabaseHeader,
        pageHeader: BTreeHeader
    ) {
        if (pageHeader.type !== "table_interior") {
            return undefined;
        }

        const offsets = this.getCellOffsets(bytes, pageHeader);
        const parsedCells = offsets.map<BTreeTablePagePointer>((offset) => {
            console.log("parsing at " + offset);
            let currentIndex = offset;
            const pageNumber = bytes.readUint32BE(currentIndex);
            offset += 4;
            return {
                pageNumber,
                key: this.readVarInt(bytes, currentIndex).value,
            };
        });

        return parsedCells;
    }

    static parseBTreePage(
        bytes: Buffer,
        pageNumber: number,
        dbHeader: DatabaseHeader
    ) {
        try {
            const bytesWithoutPageZeroHeader =
                pageNumber === 0 ? bytes.subarray(100) : bytes;
            const header = this.parseHeader(
                bytesWithoutPageZeroHeader,
                pageNumber
            );

            // testing
            if (pageNumber !== 0) {
                // && header.cellContentStartIndex !== 3418 ) {
                return undefined;
            }

            const parsed =
                header.type === "table_leaf"
                    ? this.parseBTreeTableLeaf(bytes, dbHeader, header)
                    : this.parseBTreeTableInterior(bytes, dbHeader, header);

            return { pageNumber, header, parsed };
        } catch (err) {
            console.error(err);
            return undefined;
        }
    }
}
