import _ from "lodash";
import { buffer } from "stream/consumers";
import { DatabaseFile } from "./DatabaseFile";
import { BTreeIndexInteriorData, BTreePage, BTreePageOfType, BTreePageRecord, BTreeRecord, BTreeRow, BTreeTablePagePointer } from "./DatabaseFile.types";

// Maps the page type to the value stored at byte 0x00 in the header.
const BTreePageTypeToHeaderTypeMap: { [T in BTreePage['type']]: number } = {
    index_interior: 0x02,
    table_interior: 0x05,
    index_leaf: 0x0a,
    table_leaf: 0x0d
}

export class DatabaseFileBTreePageWriter {
    constructor(private readonly database: DatabaseFile, private readonly page: BTreePage) {}

    static convertBTreeRecordTypeToSerialNumber(record: BTreeRecord): number {
        switch (record.type) {
            case 'NULL':
                return 0;
            case 'int_8': 
                return 1;
            case 'int_16': 
                return 2;
            case 'int_24': 
                return 3;
            case 'int_32': 
                return 4;
            case 'int_48': 
                return 5;
            case 'int_64': 
                return 6;
            case 'float_64': 
                return 7;
            case '0': 
                return 8;
            case '1': 
                return 9;
            case 'reserved': 
                return 11;
            case 'blob': 
                return (record.value.length + 12) * 2;
            case 'text': 
                return (record.value.length + 13) * 2;
        }
    }

    static isInteriorPage(page: BTreePage): page is BTreePageOfType<'index_interior' | 'table_interior'> {
        return page.type === 'index_interior' || page.type === 'table_interior';
    }

    static getPageHeaderSize(page: BTreePage): number {
        return DatabaseFileBTreePageWriter.isInteriorPage(page) ? 12 : 8;
    }
    
    static writeHeaderBytes(buffer: Buffer, page: BTreePage) {
        // Write the page type
        buffer.writeUint8(BTreePageTypeToHeaderTypeMap[page.type], 0x00);

        // Write the first freeblock
        buffer.writeUInt16BE(page.header.firstFreeblockIndex, 0x01);

        // Write the number of cells
        buffer.writeUInt16BE(page.header.numberCells, 0x03);

        // Write the cell contents area
        // If the index is 65536, it will be beyond what can be stored in a 2-byte value, hence we use 0.
        buffer.writeUInt16BE(page.header.cellContentStartIndex === 65536 ? 0 : page.header.cellContentStartIndex, 0x05);

        // Write the number of fragmented bytes
        buffer.writeUInt16BE(page.header.numberFragmentedFreeBytesInCellContent, 0x07);

        // If an interior page, we write the rightmost pointer.
        if (DatabaseFileBTreePageWriter.isInteriorPage(page)) {
            buffer.writeUInt32BE(page.header.rightmostPointer, 0x08)
        }

        return buffer;
    }

    static writeCellPointers(buffer: Buffer, cellOffsets: number[], startPosition: number) {
        for (let i = 0; i < cellOffsets.length; i ++) {
            console.log(`writing cell offset (${cellOffsets[i]}) to ${startPosition + (i * 2)}`)
            buffer.writeUint16BE(cellOffsets[i], startPosition + (i * 2));
        }
    }
    
    // Converts a number to a VarInt.
    // TODO: handle 9 byte values fully, as we currently will fail.
    static numToVarInt(num: number): Buffer {
        const bytes: number[] = [];

        if (num === 0) {
            return Buffer.from([0]);
        }

        if (num === 1) {
            return Buffer.from([1]);
        }
        
        if (num === 128) {
            return Buffer.from([0b1000_0001, 0b0000_0000])
        }

        // The log_2 of a number will return the most significant bit.
        // This intuitively makes sense, as if the bit higher than it was set, the number would be * 2 and thus log_2(num * 2) = 1 + log_2(num).
        const mostSignificantBitIndex = Math.floor(Math.log2(num));
        let currentBitIndex = 7 * Math.ceil((mostSignificantBitIndex ?? 1) / 7);
        while (currentBitIndex > 0) {
            const numBits = (bytes.length === 7) ? 8 : 7;
            const mask = numBits === 8 ? 0b1111_1111 : 0b0111_1111;
            const shiftDelta = numBits === 7 ? 7 : 6;
            const byte = (num >> (currentBitIndex - shiftDelta)) & mask;

            bytes.push(byte);
            currentBitIndex -= numBits;
        }
        

        const buffer = Buffer.alloc(bytes.length);
        bytes.forEach((b, idx) => buffer.writeUInt8(
            idx === 8 ? b : 
            idx === bytes.length - 1 ? (0b0000_0000 | b) : (0b1000_0000 | b), idx));

        return buffer;
    }

    static writeRecordValue(buffer: Buffer, record: BTreeRecord, currentIndex: number) {
        switch (record.type) {
            case 'NULL':
                return 0;
            case 'int_8':
                buffer.writeInt8(record.value, currentIndex);
                return 1;
            case 'int_16':
                buffer.writeInt16BE(record.value, currentIndex);
                return 2;
            case 'int_24':
                buffer.writeIntBE(record.value, currentIndex, 3);
                return 3;
            case 'int_32':
                buffer.writeInt32BE(record.value, currentIndex);
                return 4;
            case 'int_48':
                buffer.writeIntBE(record.value, currentIndex, 6);
                return 6;
            case 'int_64':
                buffer.writeBigInt64BE(BigInt(record.value), currentIndex);
                return 8;
            case 'float_64':
                buffer.writeDoubleBE(record.value, currentIndex);
                return 8;
            case '0':
                return 0;
            case '1':
                return 0;
            case 'reserved':
                return 0;
            case 'blob':
                record.value.forEach((byte, idx) => buffer.writeUint8(byte, currentIndex + idx));
                return record.value.length;
            case 'text':
                buffer.write(record.value, currentIndex);
                return record.value.length;
        }
    }

    static getSerialTypesBuffer(cell: BTreeRow) {
        const serialTypeVarInts = cell.records.map(record => DatabaseFileBTreePageWriter.numToVarInt(DatabaseFileBTreePageWriter.convertBTreeRecordTypeToSerialNumber(record)));

        const buffer = Buffer.alloc(_.sumBy(serialTypeVarInts, s => s.length));
        let currentIndex = 0;
        for (const varInt of serialTypeVarInts) {
            varInt.copy(buffer, currentIndex);
            currentIndex += varInt.length;
        }

        return buffer;
    }

    static writeTablePageLeafRecord(buffer: Buffer, cell: BTreeRow) {
        const sizeBuffer = DatabaseFileBTreePageWriter.numToVarInt(cell.payloadSize);
        const rowIdBuffer = DatabaseFileBTreePageWriter.numToVarInt(cell.rowId);
        let currentOffset = cell.pageOffset;

        console.log(`writing size ptr ${cell.payloadSize} to ${currentOffset} ${sizeBuffer.length}`);
        sizeBuffer.copy(buffer, currentOffset);
        currentOffset += sizeBuffer.length;
        console.log(`writing rowId ${cell.rowId} to ${currentOffset}`);
        rowIdBuffer.copy(buffer, currentOffset);
        currentOffset += rowIdBuffer.length;

        const contentAreaBuffer = Buffer.alloc(cell.payloadSize);

        const serialTypesBuffer = DatabaseFileBTreePageWriter.getSerialTypesBuffer(cell);
        const serialTypesBufferLengthVarInt = DatabaseFileBTreePageWriter.numToVarInt(serialTypesBuffer.length);

        // Write the length of the header
        console.log(`writing row header length ${serialTypesBuffer.length} to ${currentOffset}`)
        serialTypesBufferLengthVarInt.copy(buffer, currentOffset);
        currentOffset += serialTypesBufferLengthVarInt.length;
        
        // Write the header
        console.log(`writing row header ${serialTypesBuffer} to ${currentOffset}`)
        serialTypesBuffer.copy(buffer, currentOffset);
        currentOffset += serialTypesBuffer.length;

        // Write the body
        let contentAreaCurrentIndex = 0;
        for (const record of cell.records) {
            contentAreaCurrentIndex += DatabaseFileBTreePageWriter.writeRecordValue(contentAreaBuffer, record, contentAreaCurrentIndex);
        }

        const contentAreaFinalSize = cell.storedSize - serialTypesBufferLengthVarInt.length - serialTypesBuffer.length;
        console.log(`copying ${contentAreaFinalSize} (${contentAreaBuffer.length}) content-area bytes to ${currentOffset}`);
        contentAreaBuffer.copy(buffer, currentOffset, 0, contentAreaFinalSize);

        currentOffset += contentAreaFinalSize;

        if (cell.overflowPage && 2 === 5 - 5) {
            buffer.writeUInt32BE(cell.overflowPage, currentOffset);
        }
    }

    static writeTablePagePointerRecord(buffer: Buffer, cell: BTreeTablePagePointer, lastRecordOffset: number) {
        const keyBuffer = DatabaseFileBTreePageWriter.numToVarInt(cell.key);
        const startOffset = lastRecordOffset - 4 - keyBuffer.length;

        buffer.writeUInt32BE(cell.pageNumber, startOffset);
        keyBuffer.copy(buffer, startOffset + 4);
    }

    getBytesBuffer(): Buffer {
        const buffer = Buffer.alloc(this.database.header.pageSizeBytes);
        DatabaseFileBTreePageWriter.writeHeaderBytes(buffer, this.page);
        
        switch (this.page.type) {
            case 'index_interior':
                throw new Error();
            case 'index_leaf':
                throw new Error();
            case 'table_interior':
                throw new Error();
            case 'table_leaf':
                console.log('header', this.page.header);
                console.log('header offsets', this.page.rows.map(r => r.pageOffset))
                DatabaseFileBTreePageWriter.writeCellPointers(buffer, this.page.rows.map(r => r.pageOffset), DatabaseFileBTreePageWriter.getPageHeaderSize(this.page));
                _.sortBy(this.page.rows, row => row.pageOffset).forEach(row => {
                    DatabaseFileBTreePageWriter.writeTablePageLeafRecord(buffer, row);
                })
        }
        
        return buffer;
    }

}