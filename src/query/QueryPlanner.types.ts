export type ResultSetCommon = {
    data: Record<string, any[]>;
}

export type ResultSet = { [tableName: string]: any }[];
