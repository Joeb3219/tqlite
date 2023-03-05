export type FileFormatVersion = "legacy" | "WAL";
export type TextEncoding = "utf8" | "utf16le" | "utf16be";

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
};

export type BTree =
    | {
          type: "table";
      }
    | {
          type: "index";
      };

export type BTreeType =
    | "index_interior"
    | "index_leaf"
    | "table_interior"
    | "table_leaf";

export type BTreeHeaderCommon = {
    firstFreeblockIndex: number;
    numberCells: number;
    cellContentStartIndex: number;
    numberFragmentedFreeBytesInCellContent: number;
    pageNumber: number;
};
export type BTreeHeader =
    | ({
        type: "index_leaf";
    } & BTreeHeaderCommon)
    | ({
        type: "table_leaf";
    } & BTreeHeaderCommon)
    | ({
        type: "index_interior";
        rightmostPointer: number;
    } & BTreeHeaderCommon)
    | ({
        type: "table_interior";
        rightmostPointer: number;
    } & BTreeHeaderCommon);

export type BTreePageType = BTreeHeader["type"];
export type BTreePageHeaderOfType<T extends BTreePageType> = Extract<BTreeHeader, { type: T }>;

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
    pageOffset: number; // How many bytes into the page this record starts at
};

export type BTreeRow = {
    rowId: number;
    overflowPage: number | undefined;
    payloadSize: number;
    storedSize: number;
    records: BTreeRecord[];
    pageOffset: number; // How many bytes into the page this record starts at
    cells: any;
};

export type BTreeIndexInteriorData = {
    pageNumber: number;
    overflowPage: number | undefined;
    payloadSize: number;
    storedSize: number;
    records: BTreeRecord[];
    pageOffset: number; // How many bytes into the page this record starts at
    cells: any;
};

export type BTreeIndexData = {
    payloadSize: number;
    storedSize: number;
    overflowPage: number | undefined;
    records: BTreeRecord[];
    pageOffset: number; // How many bytes into the page this record starts at
    cells: any;
};

export type BTreePage =
    | {
          type: "table_leaf";
          rows: BTreeRow[];
          header: BTreePageHeaderOfType<"table_leaf">;
      }
    | {
          type: "table_interior";
          pointers: BTreeTablePagePointer[];
          header: BTreePageHeaderOfType<"table_interior">;
      }
    | {
          type: "index_leaf";
          indices: BTreeIndexData[];
          header: BTreePageHeaderOfType<"index_leaf">;
      }
    | {
          type: "index_interior";
          indices: BTreeIndexInteriorData[];
          header: BTreePageHeaderOfType<"index_interior">;
      };

export type BTreePageOfType<T extends BTreePage["type"]> = Extract<
    BTreePage,
    { type: T }
>;

export type BTreePageRecord = BTreeRow | BTreeTablePagePointer | BTreeIndexData | BTreeIndexInteriorData;