import _ from "lodash";
import { parse } from "path";
import { BTree, DatabaseHeader } from "./DatabaseFile.types";

export type BTreeType = 'index_interior' | 'index_leaf' | 'table_interior' | 'table_leaf';

type BTreeHeaderCommon = {
    firstFreeblockIndex: number;
    numberCells: number;
    cellContentStartIndex: number;
    numberFragmentedFreeBytesInCellContent: number;
}

export type BTreeHeader = ({
    type: 'index_interior' | 'index_leaf' | 'table_leaf';
} & BTreeHeaderCommon) | ({ 
    type: 'table_interior';
    rightmostPointer: number;
} & BTreeHeaderCommon);

export type BTreePageType = BTreeHeader['type'];

export class DatabaseFileBTreePageUtil {
    static parsePageType(bytes: Buffer): BTreePageType {
        const value = bytes.readUint8(0);

        if (value === 2) {
            return 'index_interior';
        }

        if (value === 5) {
            return 'table_interior';
        }

        if (value === 10) {
            return 'index_leaf';
        }

        if (value === 13) {
            return 'table_leaf';
        }

        throw new Error(`Page type must be either 2, 5, 10, or 13, but found ${value}`);
    }

    static parseHeader(bytes: Buffer): BTreeHeader {
        const pageType = this.parsePageType(bytes);

        const startIdx = bytes.readUint16BE(5);
        const commonData: BTreeHeaderCommon = {
            firstFreeblockIndex: bytes.readUint16BE(1),
            numberCells: bytes.readUint16BE(3),
            cellContentStartIndex: startIdx === 0 ? 65536 : startIdx,
            numberFragmentedFreeBytesInCellContent: bytes.readUint8(7)
        }

        if (pageType === 'table_interior') {
            return {
                type: 'table_interior',
                ...commonData,
                rightmostPointer: bytes.readUInt32BE(8)
            }
        }

        return {
            type: pageType,
            ...commonData
        }
    }

    // https://www.sqlite.org/src4/doc/trunk/www/varint.wiki
    // Will be in the range of 0...(2^64) - 1
    static readVarInt(bytes: Buffer, idx: number): { value: number, length: number } {
        const candidateBytes = bytes.subarray(idx, idx + 9);
        const firstByte = candidateBytes.readUInt8(0);

        if (firstByte >= 0 && firstByte <= 240) {
            return { value: firstByte, length: 1 };
        }

        if (firstByte >= 241 && firstByte <= 248) {
            return { value: 240 + 256 * (firstByte - 241) + candidateBytes.readUInt8(1), length: 2 };
        }

        if (firstByte === 249) {
            return { value: 2288 + 256 * candidateBytes.readUInt8(1) + candidateBytes.readUInt8(2), length: 4 };
        }

        const numBytes = firstByte - 250 + 3;
        return { value: _.range(0, numBytes).reduce((state, idx) => {
            return (state << 8) | candidateBytes.readUint8(idx); 
        }, 0x00), length: numBytes + 1 };
    }

    static parseBTreeTableLeaf(bytes: Buffer, dbHeader: DatabaseHeader, pageHeader: BTreeHeader) {
        if (pageHeader.type !== 'table_leaf') {
            return undefined;
        }

        const cellPointerBytes = bytes.subarray(pageHeader.cellContentStartIndex, pageHeader.cellContentStartIndex + pageHeader.numberCells * 4);
        const offsets = _.range(0, pageHeader.numberCells + 2).map(idx => cellPointerBytes.readUint16BE(idx));
        console.log(offsets);

        let currentIndex = pageHeader.cellContentStartIndex;
        const parsedCells: any[] = [];
        console.log(pageHeader);
        while (parsedCells.length < pageHeader.numberCells && currentIndex > 0) {
            // console.log(currentIndex);
            const { value: payloadSize, length: payloadSizeLength } = this.readVarInt(bytes, currentIndex);
            currentIndex += payloadSizeLength;

            const { value: rowId, length: rowIdLength } = this.readVarInt(bytes, currentIndex);   
            currentIndex += rowIdLength;

            const usableSize = dbHeader.pageSizeBytes - dbHeader.unusedReservePageSpace;
            const maxPayload = usableSize - 35;
            const minPayload = ((usableSize-12)*32/255)-23;
            const K = minPayload+((payloadSize-minPayload)%(usableSize-4));
            const storedSize = Math.floor(payloadSize <= maxPayload ? payloadSize : (payloadSize > maxPayload && K <= maxPayload) ? K : minPayload);

            // console.log({ usableSize, maxPayload, minPayload, K, payloadSize, storedSize });

            const data = bytes.subarray(currentIndex, currentIndex + storedSize);
            currentIndex += storedSize;// - payloadSizeLength - rowIdLength;

            const hasOverflow = storedSize < payloadSize;
            const overflowPage = hasOverflow ? bytes.readUint32BE(currentIndex) : undefined;
            if (hasOverflow) {
                currentIndex += 4;
            }            

            const dataStr = _.map(data, d => String.fromCharCode(d)).join('');
            const cell = { payloadSize, rowId, data: data.toString('utf8'), overflowPage };
            parsedCells.push(cell);
            console.log(cell);
        }

console.log(parsedCells);

        return parsedCells;


    }

    static parseBTreePage(bytes: Buffer, pageNumber: number, dbHeader: DatabaseHeader) {
        const header = this.parseHeader(pageNumber === 0 ? bytes.subarray(100) : bytes);

        const parsed = this.parseBTreeTableLeaf(bytes, dbHeader, header);
        
        return { header, parsed };
    }
}