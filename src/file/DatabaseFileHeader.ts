import { File } from './File';

import { DatabaseHeader, FileFormatVersion, TextEncoding } from './DatabaseFile.types';

export class DatabaseFileHeaderUtil {
    static readHeaderString(file: File): string {
        return file.readString(0, 16);
    }

    // Must be a power of two between 512 and 32768, or 1 to represent 65536.
    static readPageSizeBytes(file: File): number {
        const value = file.readInt16(16);
        
        if (value === 1) {
            return 65536;
        }

        if (value < 512 || value > 32768) {
            throw new Error(`Page size must be between 512 and 32768, but received ${value}`);
        }

        if (value % 2 !== 0) {
            throw new Error(`Page size must be a multiple of 2, but received ${value}`);
        }

        return value;
    }

    static readFileFormatVersion(file: File, variant: 'read' | 'write'): FileFormatVersion {
        const value = file.readInt8(variant === 'write' ? 18 : 19);

        if (value === 1) {
            return 'legacy';
        }

        if (value === 2) {
            return 'WAL';
        }

        throw new Error(`File format version must be either 1 or 2, but received ${value}`);
    }

    static readUnusedReservePageSpace(file: File): number {
        return file.readInt8(20);
    }

    static readEmbeddedPayloadFraction(file: File, variant: 'maximum' | 'minimum'): number {
        const value = file.readInt8(variant === 'minimum' ? 22 : 21);

        if (variant === 'maximum' && value !== 64) {
            throw new Error(`Maximum embedded payload fraction must be 64, but received ${value}`);
        }

        if (variant === 'minimum' && value !== 32) {
            throw new Error(`Maximum embedded payload fraction must be 64, but received ${value}`);
        }

        return value;
    }

    static readLeafPayloadFraction(file: File): 32 {
        const value = file.readInt8(23);

        if (value !== 32) {
            throw new Error(`Leaf payload fraction must be 32, but received ${value}`);
        }

        return value;
    }

    static readSchemaFormatNumber(file: File): 1 | 2 | 3 | 4 {
        const value = file.readInt32(44);

        if (value !== 1 && value !== 2 && value !== 3 && value !== 4) {
            throw new Error(`Schema format number must be between 1 and 4, but received ${value}`);
        }

        return value;
    }

    static readTextEncoding(file: File): TextEncoding {
        const value = file.readInt32(56);

        if (value === 1) {
            return 'utf8';
        }

        if (value === 2) {
            return 'utf16le';
        }

        if (value === 3) {
            return 'utf16be';
        }

        throw new Error(`Text encoding must be a value between 1 and 3, but received ${value}`);
    }

    static readIncrementalVacuumMode(file: File): boolean {
        const value = file.readInt32(64);

        // Non-zero means true, whereas 0 means false. 
        return value !== 0;
    }

    static parseHeader(file: File): DatabaseHeader {
        return {
            headerString: this.readHeaderString(file),
            pageSizeBytes: this.readPageSizeBytes(file),
            fileFormatWriteVersion: this.readFileFormatVersion(file, 'write'),
            fileFormatReadVersion: this.readFileFormatVersion(file, 'read'),
            unusedReservePageSpace: this.readUnusedReservePageSpace(file),
            maximumEmbeddedPayloadFraction: this.readEmbeddedPayloadFraction(file, 'maximum'),
            minimumEmbeddedPayloadFraction: this.readEmbeddedPayloadFraction(file, 'minimum'),
            leafPayloadFraction: this.readLeafPayloadFraction(file),
            fileChangeCounter: file.readInt32(24),
            databaseFileSizeInPages: file.readInt32(28),
            firstFreelistTrunkPagePageNumber: file.readInt32(32),
            numberFreelistPages: file.readInt32(36),
            schemaCookie: file.readInt32(40),
            schemaFormatNumber: this.readSchemaFormatNumber(file),
            defaultPageCacheSize: file.readInt32(48),
            largeRootBTreePagePageNumber: file.readInt32(52),
            textEncoding: this.readTextEncoding(file),
            userVersion: file.readInt32(60),
            incrementalVacuumMode: this.readIncrementalVacuumMode(file),
            applicationId: file.readInt32(68),
            versionValidForNumber: file.readInt32(92),
            sqliteVersionNumber: file.readInt32(96)
        }
    }
}