import _ from "lodash";

export class File {
    data: Buffer;

    constructor(private readonly bytes: ArrayBuffer | Buffer) {
        this.data = Buffer.from(bytes);
    }

    readString(startPos: number, length: number): string {
        const subset = _.slice(this.data, startPos, startPos + length);
        return subset.map((c) => String.fromCharCode(c)).join("");
    }

    readInt16(idx: number) {
        return this.data.readUInt16BE(idx);
    }

    readInt32(idx: number) {
        return this.data.readUInt32BE(idx);
    }

    readInt8(idx: number) {
        return this.data.readUInt8(idx);
    }
}
