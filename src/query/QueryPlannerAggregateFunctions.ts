import _ from "lodash";
import { ResultSet } from "./QueryPlanner.types";

// TODO: complete these
export const AggregateFunctionsMap: {
    [operator in string]: (
        resultSet: ResultSet,
        ...args: any[]
    ) => string | number;
} = {
    avg: (_resultSet: ResultSet, values: any) => {
        if (!Array.isArray(values)) {
            throw new Error("Expected array of values");
        }

        return _.mean(
            values.filter((c) => {
                if (typeof c === "number" || typeof c === "bigint") {
                    return true;
                }

                return false;
            })
        );
    },
    count: (_resultSet: ResultSet, values: any) => {
        if (!Array.isArray(values)) {
            throw new Error("Expected array of values");
        }

        return values.length;
    },
    group_concat: (_resultSet: ResultSet, values: any, glue?: any) => {
        if (!Array.isArray(values)) {
            throw new Error(
                "Expected array of values and optional glue parameter"
            );
        }

        return values.join(glue);
    },
    max: (_resultSet: ResultSet, values: any) => {
        if (!Array.isArray(values)) {
            throw new Error("Expected array of values");
        }

        return _.max(
            values.filter((c) => {
                if (typeof c === "number" || typeof c === "bigint") {
                    return true;
                }

                return false;
            })
        );
    },
    min: (_resultSet: ResultSet, values: any) => {
        if (!Array.isArray(values)) {
            throw new Error("Expected array of values");
        }

        return _.min(
            values.filter((c) => {
                if (typeof c === "number" || typeof c === "bigint") {
                    return true;
                }

                return false;
            })
        );
    },
    sum: (_resultSet: ResultSet, values: any) => {
        if (!Array.isArray(values)) {
            throw new Error("Expected array of values");
        }

        return _.sum(
            values.filter((c) => {
                if (typeof c === "number" || typeof c === "bigint") {
                    return true;
                }

                return false;
            })
        );
    },
    total: (_resultSet: ResultSet, values: any) => {
        if (!Array.isArray(values)) {
            throw new Error("Expected array of values");
        }

        return _.sum(
            values.filter((c) => {
                if (typeof c === "number" || typeof c === "bigint") {
                    return true;
                }

                return false;
            })
        );
    },
};

export class QueryPlannerAggregateFunctions {
    static hasFunction(name: string) {
        return !!AggregateFunctionsMap[name.toLowerCase()];
    }

    static executeFunction(name: string, resultSet: ResultSet, args: any[]) {
        const fn = AggregateFunctionsMap[name.toLowerCase()];

        return fn.call(fn, resultSet, ...args);
    }
}
