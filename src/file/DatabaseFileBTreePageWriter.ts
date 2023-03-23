import _ from "lodash";
import { DatabaseFile } from "./DatabaseFile";
import {
    BTreeIndexData,
    BTreeIndexInteriorData,
    BTreePage,
    BTreePageOfType,
    BTreeRecord,
    BTreeRow,
    BTreeTablePagePointer,
} from "./DatabaseFile.types";
import { DatabaseFileHeaderUtil } from "./DatabaseFileHeader";

// Maps the page type to the value stored at byte 0x00 in the header.
const BTreePageTypeToHeaderTypeMap: { [T in BTreePage["type"]]: number } = {
    index_interior: 0x02,
    table_interior: 0x05,
    index_leaf: 0x0a,
    table_leaf: 0x0d,
};

export class DatabaseFileBTreePageWriter {
    constructor(
        private readonly database: DatabaseFile,
        private readonly page: BTreePage
    ) {}

    static convertBTreeRecordTypeToSerialNumber(record: BTreeRecord): number {
        switch (record.type) {
            case "NULL":
                return 0;
            case "int_8":
                return 1;
            case "int_16":
                return 2;
            case "int_24":
                return 3;
            case "int_32":
                return 4;
            case "int_48":
                return 5;
            case "int_64":
                return 6;
            case "float_64":
                return 7;
            case "0":
                return 8;
            case "1":
                return 9;
            case "reserved":
                return 11;
            case "blob":
                return record.value.length * 2 + 12;
            case "text":
                return record.value.length * 2 + 13;
        }
    }

    static isInteriorPage(
        page: BTreePage
    ): page is BTreePageOfType<"index_interior" | "table_interior"> {
        return page.type === "index_interior" || page.type === "table_interior";
    }

    static getPageHeaderSize(page: BTreePage): number {
        const pageHeaderOffset = DatabaseFileBTreePageWriter.isInteriorPage(
            page
        )
            ? 12
            : 8;
        return page.header.pageNumber === 0
            ? pageHeaderOffset + 100
            : pageHeaderOffset;
    }

    writeHeaderBytes(buffer: Buffer, page: BTreePage) {
        const isFirstPage = page.header.pageNumber === 0;
        const firstPageOffset = isFirstPage ? 100 : 0;

        // If needed, writes the database header
        // The database header is only written if there's
        if (page.header.pageNumber === 0) {
            DatabaseFileHeaderUtil.writeHeader(buffer, this.database.header);
        }

        // Write the page type
        buffer.writeUint8(
            BTreePageTypeToHeaderTypeMap[page.type],
            firstPageOffset + 0x00
        );

        // Write the first freeblock
        buffer.writeUInt16BE(
            page.header.firstFreeblockIndex,
            firstPageOffset + 0x01
        );

        // Write the number of cells
        buffer.writeUInt16BE(page.header.numberCells, firstPageOffset + 0x03);

        // Write the cell contents area
        // If the index is 65536, it will be beyond what can be stored in a 2-byte value, hence we use 0.
        buffer.writeUInt16BE(
            page.header.cellContentStartIndex === 65536
                ? 0
                : page.header.cellContentStartIndex,
            firstPageOffset + 0x05
        );

        // Write the number of fragmented bytes
        buffer.writeUInt16BE(
            page.header.numberFragmentedFreeBytesInCellContent,
            firstPageOffset + 0x07
        );

        // If an interior page, we write the rightmost pointer.
        if (DatabaseFileBTreePageWriter.isInteriorPage(page)) {
            buffer.writeUInt32BE(
                page.header.rightmostPointer,
                firstPageOffset + 0x08
            );
        }

        return buffer;
    }

    static writeCellPointers(
        buffer: Buffer,
        cellOffsets: number[],
        startPosition: number
    ) {
        for (let i = 0; i < cellOffsets.length; i++) {
            buffer.writeUint16BE(cellOffsets[i], startPosition + i * 2);
        }
    }

    // Converts a number to a VarInt.
    // TODO: handle 9 byte values fully, as we currently will fail.
    static numToVarInt(num: number): Buffer {
        if (num === 0) {
            return Buffer.from([0]);
        }

        // 0b1000_0001
        // chunks into [0b0000_001, and 0b0000_001]
        const byteOctets = [
            // TODO: support > 32 bit values
            // (num >> 56) & 0b1111_1111,
            // (num >> 49) & 0b0111_1111,
            // (num >> 42) & 0b0111_1111,
            // (num >> 35) & 0b0111_1111,
            (num >> 28) & 0b0111_1111,
            (num >> 21) & 0b0111_1111,
            (num >> 14) & 0b0111_1111,
            (num >> 7) & 0b0111_1111,
            (num >> 0) & 0b0111_1111,
        ];
        // We can trim anything to the left of the MSB.
        const mostSignificantByteIndex = byteOctets.findIndex(
            (byte) => byte !== 0
        );
        const bytes = byteOctets.slice(mostSignificantByteIndex);

        const buffer = Buffer.alloc(bytes.length);
        bytes.forEach((b, idx) =>
            buffer.writeUInt8(
                idx === 8
                    ? b
                    : idx === bytes.length - 1
                    ? 0b0000_0000 | b
                    : 0b1000_0000 | b,
                idx
            )
        );

        return buffer;
    }

    static writeRecordValue(
        buffer: Buffer,
        record: BTreeRecord,
        currentIndex: number
    ) {
        switch (record.type) {
            case "NULL":
                return 0;
            case "int_8":
                buffer.writeInt8(record.value, currentIndex);
                return 1;
            case "int_16":
                buffer.writeInt16BE(record.value, currentIndex);
                return 2;
            case "int_24":
                buffer.writeIntBE(record.value, currentIndex, 3);
                return 3;
            case "int_32":
                buffer.writeInt32BE(record.value, currentIndex);
                return 4;
            case "int_48":
                buffer.writeIntBE(record.value, currentIndex, 6);
                return 6;
            case "int_64":
                buffer.writeBigInt64BE(BigInt(record.value), currentIndex);
                return 8;
            case "float_64":
                buffer.writeDoubleBE(record.value, currentIndex);
                return 8;
            case "0":
                return 0;
            case "1":
                return 0;
            case "reserved":
                return 0;
            case "blob":
                record.value.forEach((byte, idx) =>
                    buffer.writeUint8(byte, currentIndex + idx)
                );
                return record.value.length;
            case "text":
                buffer.write(record.value, currentIndex);
                return record.value.length;
        }
    }

    static getSerialTypesBuffer(records: BTreeRecord[]) {
        const serialTypeVarInts = records.map((record) =>
            DatabaseFileBTreePageWriter.numToVarInt(
                DatabaseFileBTreePageWriter.convertBTreeRecordTypeToSerialNumber(
                    record
                )
            )
        );

        const buffer = Buffer.alloc(
            _.sumBy(serialTypeVarInts, (s) => s.length)
        );
        let currentIndex = 0;
        for (const varInt of serialTypeVarInts) {
            varInt.copy(buffer, currentIndex);
            currentIndex += varInt.length;
        }

        return buffer;
    }

    static writeTablePagePointer(buffer: Buffer, cell: BTreeTablePagePointer) {
        let currentIndex: number = cell.pageOffset;

        buffer.writeUInt32BE(cell.pageNumber, currentIndex);
        currentIndex += 4;

        const varIntBuffer = DatabaseFileBTreePageWriter.numToVarInt(cell.key);
        varIntBuffer.copy(buffer, currentIndex);
    }

    static writeIndexLeafRecord(buffer: Buffer, cell: BTreeIndexData) {
        let currentOffset: number = cell.pageOffset;
        const sizeBuffer = DatabaseFileBTreePageWriter.numToVarInt(
            cell.payloadSize
        );

        sizeBuffer.copy(buffer, currentOffset);
        currentOffset += sizeBuffer.length;

        const contentAreaBuffer = Buffer.alloc(cell.payloadSize);

        const serialTypesBuffer =
            DatabaseFileBTreePageWriter.getSerialTypesBuffer(cell.records);
        const serialTypesBufferLengthVarInt =
            DatabaseFileBTreePageWriter.numToVarInt(
                serialTypesBuffer.length + 1
            );

        // Write the length of the header
        serialTypesBufferLengthVarInt.copy(buffer, currentOffset);
        currentOffset += serialTypesBufferLengthVarInt.length;

        // Write the header
        serialTypesBuffer.copy(buffer, currentOffset);
        currentOffset += serialTypesBuffer.length;

        // Write the body
        let contentAreaCurrentIndex = 0;
        for (const record of cell.records) {
            contentAreaCurrentIndex +=
                DatabaseFileBTreePageWriter.writeRecordValue(
                    contentAreaBuffer,
                    record,
                    contentAreaCurrentIndex
                );
        }

        const contentAreaFinalSize =
            cell.storedSize -
            serialTypesBufferLengthVarInt.length -
            serialTypesBuffer.length;
        contentAreaBuffer.copy(buffer, currentOffset, 0, contentAreaFinalSize);

        currentOffset += contentAreaFinalSize;

        if (cell.overflowPage) {
            buffer.writeUInt32BE(cell.overflowPage, currentOffset);
        }
    }

    static writeTablePageLeafRecord(buffer: Buffer, cell: BTreeRow) {
        const sizeBuffer = DatabaseFileBTreePageWriter.numToVarInt(
            cell.payloadSize
        );
        const rowIdBuffer = DatabaseFileBTreePageWriter.numToVarInt(cell.rowId);
        let currentOffset = cell.pageOffset;

        sizeBuffer.copy(buffer, currentOffset);
        currentOffset += sizeBuffer.length;

        rowIdBuffer.copy(buffer, currentOffset);
        currentOffset += rowIdBuffer.length;

        const contentAreaBuffer = Buffer.alloc(cell.payloadSize);

        const serialTypesBuffer =
            DatabaseFileBTreePageWriter.getSerialTypesBuffer(cell.records);
        const serialTypesBufferLengthVarInt =
            DatabaseFileBTreePageWriter.numToVarInt(
                serialTypesBuffer.length + 1
            );

        // Write the length of the header
        serialTypesBufferLengthVarInt.copy(buffer, currentOffset);
        currentOffset += serialTypesBufferLengthVarInt.length;

        // Write the header
        serialTypesBuffer.copy(buffer, currentOffset);
        currentOffset += serialTypesBuffer.length;

        // Write the body
        let contentAreaCurrentIndex = 0;
        for (const record of cell.records) {
            contentAreaCurrentIndex +=
                DatabaseFileBTreePageWriter.writeRecordValue(
                    contentAreaBuffer,
                    record,
                    contentAreaCurrentIndex
                );
        }

        const contentAreaFinalSize =
            cell.storedSize -
            serialTypesBufferLengthVarInt.length -
            serialTypesBuffer.length;
        contentAreaBuffer.copy(buffer, currentOffset, 0, contentAreaFinalSize);

        currentOffset += contentAreaFinalSize;

        if (cell.overflowPage) {
            buffer.writeUInt32BE(cell.overflowPage, currentOffset);
        }
    }

    static writeIndexInteriorRecord(
        buffer: Buffer,
        cell: BTreeIndexInteriorData
    ) {
        let currentOffset: number = cell.pageOffset;

        // Page number
        buffer.writeUInt32BE(cell.pageNumber, currentOffset);
        currentOffset += 4;

        // Payload size
        const sizeBuffer = DatabaseFileBTreePageWriter.numToVarInt(
            cell.payloadSize
        );
        sizeBuffer.copy(buffer, currentOffset);
        currentOffset += sizeBuffer.length;

        const contentAreaBuffer = Buffer.alloc(cell.payloadSize);

        const serialTypesBuffer =
            DatabaseFileBTreePageWriter.getSerialTypesBuffer(cell.records);
        const serialTypesBufferLengthVarInt =
            DatabaseFileBTreePageWriter.numToVarInt(
                serialTypesBuffer.length + 1
            );

        // Write the length of the header
        serialTypesBufferLengthVarInt.copy(buffer, currentOffset);
        currentOffset += serialTypesBufferLengthVarInt.length;

        // Write the header
        serialTypesBuffer.copy(buffer, currentOffset);
        currentOffset += serialTypesBuffer.length;

        // Write the body
        let contentAreaCurrentIndex = 0;
        for (const record of cell.records) {
            contentAreaCurrentIndex +=
                DatabaseFileBTreePageWriter.writeRecordValue(
                    contentAreaBuffer,
                    record,
                    contentAreaCurrentIndex
                );
        }

        const contentAreaFinalSize =
            cell.storedSize -
            serialTypesBufferLengthVarInt.length -
            serialTypesBuffer.length;
        contentAreaBuffer.copy(buffer, currentOffset, 0, contentAreaFinalSize);

        currentOffset += contentAreaFinalSize;
    }

    static writeTablePagePointerRecord(
        buffer: Buffer,
        cell: BTreeTablePagePointer,
        lastRecordOffset: number
    ) {
        const keyBuffer = DatabaseFileBTreePageWriter.numToVarInt(cell.key);
        const startOffset = lastRecordOffset - 4 - keyBuffer.length;

        buffer.writeUInt32BE(cell.pageNumber, startOffset);
        keyBuffer.copy(buffer, startOffset + 4);
    }

    getBytesBuffer(): Buffer {
        const buffer = Buffer.alloc(this.database.header.pageSizeBytes);
        this.writeHeaderBytes(buffer, this.page);

        switch (this.page.type) {
            case "index_interior": {
                const lastPointer = _.last(this.page.indices);
                DatabaseFileBTreePageWriter.writeCellPointers(
                    buffer,
                    // We do not write the last pointer since it will be stored at the 8th byte
                    this.page.indices.slice(0, -1).map((r) => r.pageOffset),
                    DatabaseFileBTreePageWriter.getPageHeaderSize(this.page)
                );
                // Write the rightmost pointer to the 8th byte
                if (lastPointer) {
                    buffer.writeUint32BE(lastPointer.pageNumber, 8);
                }
                _.sortBy(
                    this.page.indices.slice(0, -1),
                    (pointer) => pointer.pageOffset
                ).forEach((pointer) => {
                    DatabaseFileBTreePageWriter.writeIndexInteriorRecord(
                        buffer,
                        pointer
                    );
                });
                break;
            }
            case "index_leaf": {
                DatabaseFileBTreePageWriter.writeCellPointers(
                    buffer,
                    this.page.indices.map((r) => r.pageOffset),
                    DatabaseFileBTreePageWriter.getPageHeaderSize(this.page)
                );
                _.sortBy(this.page.indices, (row) => row.pageOffset).forEach(
                    (row) => {
                        DatabaseFileBTreePageWriter.writeIndexLeafRecord(
                            buffer,
                            row
                        );
                    }
                );
                break;
            }
            case "table_interior": {
                const lastPointer = _.last(this.page.pointers);
                DatabaseFileBTreePageWriter.writeCellPointers(
                    buffer,
                    // We do not write the last pointer since it will be stored at the 8th byte
                    this.page.pointers.slice(0, -1).map((r) => r.pageOffset),
                    DatabaseFileBTreePageWriter.getPageHeaderSize(this.page)
                );

                // Write the rightmost pointer to the 8th byte
                if (lastPointer) {
                    buffer.writeUint32BE(lastPointer.pageNumber, 8);
                }

                _.sortBy(
                    this.page.pointers.slice(0, -1),
                    (pointer) => pointer.pageOffset
                ).forEach((pointer) => {
                    DatabaseFileBTreePageWriter.writeTablePagePointer(
                        buffer,
                        pointer
                    );
                });
                break;
            }
            case "table_leaf": {
                DatabaseFileBTreePageWriter.writeCellPointers(
                    buffer,
                    this.page.rows.map((r) => r.pageOffset),
                    DatabaseFileBTreePageWriter.getPageHeaderSize(this.page)
                );
                _.sortBy(this.page.rows, (row) => row.pageOffset).forEach(
                    (row) => {
                        DatabaseFileBTreePageWriter.writeTablePageLeafRecord(
                            buffer,
                            row
                        );
                    }
                );
                break;
            }
        }

        return buffer;
    }
}
