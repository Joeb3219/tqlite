
export type FileFormatVersion = 'legacy' | 'WAL';
export type TextEncoding = 'utf-8' | 'utf-16le' | 'utf-16be';

export type DatabaseHeader = {
    headerString: string;
    pageSizeBytes: number;
    fileFormatWriteVersion: FileFormatVersion;
    fileFormatReadVersion: FileFormatVersion;
    unusedReservePageSpace: number;
    maximumEmbeddedPayloadFraction: number;
    minimumEmbeddedPayloadFraction: number;
    leafPayloadFraction: number;
    fileChangeCounter: number;
    databaseFileSizeInPages: number;
    firstFreelistTrunkPagePageNumber: number;
    numberFreelistPages: number;
    schemaCookie: number;
    schemaFormatNumber: 1 | 2 | 3 | 4;
    defaultPageCacheSize: number;
    largeRootBTreePagePageNumber: number;
    textEncoding: TextEncoding;
    userVersion: number;
    incrementalVacuumMode: boolean;
    applicationId: number;
    versionValidForNumber: number;
    sqliteVersionNumber: number;
}