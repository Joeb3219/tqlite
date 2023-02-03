import { ResultSet } from "./QueryPlanner.types";

// TODO: complete these
export const AggregateFunctionsMap: {
    [operator in string]: (resultSet: ResultSet, ...args: any[]) => string | number;
} = {
    avg: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    },
    count: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    },
    group_concat: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    },
    max: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    },
    min: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    },
    sum: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    },
    total: (resultSet: ResultSet, ...args: any[]) => {
        return 0;
    }
};

export class QueryPlannerAggregateFunctions {
    static hasFunction(name: string) {
        return !!AggregateFunctionsMap[name];
    }

    static executeFunction(name: string, resultSet: ResultSet, args: any[]) {
        const fn = AggregateFunctionsMap[name];

        return fn.call(fn, resultSet, ...args);
    }
}
