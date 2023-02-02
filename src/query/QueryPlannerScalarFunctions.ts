import _ from "lodash";

export const ScalarFunctionsMap: {
    [operator in string]: (...args: any[]) => any;
} = {
    abs: (a) => Math.abs(a),
    changes: () => {
        throw new Error("Unimplemented function `changes`.");
    },
    char: (...args: any[]) => args.map((c) => String.fromCharCode(c)).join(""),
    coalesce: (...args: any[]) => {
        if (args.length < 2) {
            throw new Error("Coalesce must have at least 2 arguments");
        }

        return args.find((a) => !_.isNil(a));
    },
    format: (format: any, ...args: any[]) => {
        throw new Error("Unimplemented function `format`.");
    },
    glob: (a: any, b: any) => {
        throw new Error("Unimplemented function `glob`.");
    },
    blob: (a: any) => {
        throw new Error("Unimplemented function `blob`.");
    },
    ifnull: (...args: any[]) => {
        if (args.length !== 2) {
            throw new Error("ifnull must have exactly 2 arguments");
        }

        return args.find((a) => !_.isNil(a));
    },
    iif: (comparator: any, ifTrue: any, ifFalse: any) =>
        comparator ? ifTrue : ifFalse,
    instr: (a: any, b: any) => {
        throw new Error("Unimplemented function `instr`.");
    },
    last_insert_rowid: () => {
        throw new Error("Unimplemented function `last_insert_rowid`.");
    },
    length: (a: any) => {
        if (typeof a !== "string" && !Array.isArray(a)) {
            throw new Error("length is only defined for strings and arrays");
        }

        return a.length;
    },
    like: (a: any, b: any, c?: any) => {
        throw new Error("Unimplemented function `like`.");
    },
    likelihood: (a: any, b: any) => a,
    likely: (a: any, b: any) => a,
    load_extension: (a: any, b: any, c?: any) => {
        throw new Error("Unimplemented function `load_extension`.");
    },
    lower: (a: any) => {
        if (typeof a !== "string") {
            throw new Error("lower is only defined for strings");
        }

        return a.toLowerCase();
    },
    ltrim: (a: any, b?: any) => {
        if (typeof a !== "string") {
            throw new Error("ltrim is only defined for strings");
        }

        if (!b) {
            return a.trimStart();
        }

        // TODO: fix
        return a.replace(b, "");
    },
    max: (...args: any[]) => _.max(args),
    min: (...args: any[]) => _.min(args),
    nullif: (a: any, b: any) => (a === b ? null : a),
    printf: (format: any, ...args: any[]) => {
        throw new Error("Unimplemented function `printf`.");
    },
    quote: (a: any) => {
        throw new Error("Unimplemented function `quote`.");
    },
    random: () =>
        Math.floor(
            Math.random() *
                (Number.MAX_SAFE_INTEGER - Number.MIN_SAFE_INTEGER + 1) +
                Number.MAX_SAFE_INTEGER
        ),
    randomblob: (a: number) => {
        throw new Error("Unimplemented function `randomblob`.");
    },
    replace: (str: any, needle: any, haystack: any) => {
        if (
            typeof str !== "string" ||
            typeof needle !== "string" ||
            typeof haystack !== "string"
        ) {
            throw new Error("Expected three string arguments");
        }

        return str.replace(new RegExp(needle), haystack);
    },
    round: (a: any, b?: any) => {
        if (
            typeof a !== "number" ||
            (b !== undefined && typeof b !== "number")
        ) {
            throw new Error("Received non-numeric arguments");
        }

        return _.round(a, b);
    },
    rtrim: (a: any, b?: any) => {
        if (typeof a !== "string") {
            throw new Error("rtrim is only defined for strings");
        }

        if (!b) {
            return a.trimEnd();
        }

        // TODO: fix
        return a.replace(b, "");
    },
    sign: (a: any) => {
        if (typeof a !== "number") {
            throw new Error("Received non-numeric argument");
        }

        return a < 0 ? -1 : a === 0 ? 0 : 1;
    },
    // This is the most obnoxious function, possibly ever
    // https://en.wikipedia.org/wiki/Soundex
    soundex: (str: any) => {
        if (typeof str !== "string") {
            throw new Error("Received non-string argument");
        }

        const letterMap: Record<string, number> = {
            b: 1,
            f: 1,
            p: 1,
            v: 1,
            c: 2,
            g: 2,
            j: 2,
            k: 2,
            q: 2,
            s: 2,
            x: 2,
            z: 2,
            d: 3,
            t: 3,
            l: 4,
            m: 5,
            n: 5,
            r: 6,
        };
        const firstLetter = str[0];
        const mappedDigits = str
            .replace(/[aeiouyhw]/g, "")
            .split("")
            .map((c) => letterMap[c] ?? 0);
        const reducedDigits = mappedDigits.reduce<number[]>(
            (state, current, idx) => {
                // This is a repeat, so we'll skip it.
                if (mappedDigits[idx - 1] === current) {
                    return state;
                }

                return [...state, current];
            },
            []
        );
        const reducedDigitsWithoutZero = reducedDigits.filter(
            (digit) => digit !== 0
        );
        const candidateDigits =
            reducedDigitsWithoutZero[0] === letterMap[firstLetter]
                ? reducedDigitsWithoutZero.slice(1)
                : reducedDigitsWithoutZero;
        const threeDigits =
            candidateDigits.length > 3
                ? candidateDigits.slice(0, 3).join("")
                : candidateDigits.join("").padStart(3, "0");

        return `${firstLetter}${threeDigits}`;
    },
    sqlite_compileoption_get: () => {
        throw new Error("Unimplemented function `sqlite_compileoption_get`.");
    },
    sqlite_compileoption_used: () => {
        throw new Error("Unimplemented function `sqlite_compileoption_used`.");
    },
    sqlite_offset: () => {
        throw new Error("Unimplemented function `sqlite_offset`.");
    },
    sqlite_source_id: () => {
        throw new Error("Unimplemented function `sqlite_source_id`.");
    },
    sqlite_version: () => {
        throw new Error("Unimplemented function `sqlite_version`.");
    },
    substr: (a: any, b: any, c?: any) => {
        if (
            typeof a !== "string" ||
            typeof b !== "number" ||
            (c && typeof c !== "number")
        ) {
            throw new Error("Invalid argument types");
        }

        return a.substring(b, b + (c ?? 0));
    },
    substring: (a: any, b: any, c?: any) => {
        if (
            typeof a !== "string" ||
            typeof b !== "number" ||
            (c && typeof c !== "number")
        ) {
            throw new Error("Invalid argument types");
        }

        return a.substring(b, b + (c ?? 0));
    },
    total_changes: () => {
        throw new Error("Unimplemented function `total_changes`.");
    },
    trim: (a: any, b?: any) => {
        if (typeof a !== "string" || (b && typeof b !== "string")) {
            throw new Error("Invalid argument types");
        }

        if (!b) {
            return a.trim();
        }

        // TODO: fix
        return a.replace(b, "");
    },
    typeof: (a: any) => {
        if (_.isNil(a)) {
            return "null";
        }

        if (typeof a === "number") {
            return _.isInteger(a) ? "integer" : "real";
        }

        if (typeof a === "string") {
            return "text";
        }

        return "blob";
    },
    unicode: (a: any) => {
        throw new Error("Unimplemented function `unicode`.");
    },
    unlikely: (x: any) => x,
    upper: (x: any) => {
        if (typeof x !== "string") {
            throw new Error("upper is only defined for strings");
        }

        return x.toUpperCase();
    },
    zeroblob: (n: any) => {
        throw new Error("Unimplemented function `zeroblob`.");
    },
};

export class QueryPlannerScalarFunctions {
    static hasFunction(name: string) {
        return !!ScalarFunctionsMap[name];
    }

    static executeFunction(name: string, args: any[]) {
        const fn = ScalarFunctionsMap[name];

        return fn.call(fn, ...args);
    }
}
