import dayjs from "dayjs";

function computeDate(timeValue?: any, ...modifiers: any[]) {
    const firstModifier = modifiers[0];
    if (
        firstModifier === "unixepoch" ||
        firstModifier === "julianday" ||
        firstModifier === "auto" ||
        firstModifier === "localtime" ||
        firstModifier === "utc"
    ) {
        throw new Error(`Modifier not yet supported ${firstModifier}`);
    }

    const baseDate = timeValue === "now" ? dayjs() : dayjs(timeValue);
    return modifiers.reduce<dayjs.Dayjs>((state, modifier) => {
        if (typeof modifier !== "string") {
            return state;
        }

        if (modifier === "start of month") {
            return state.startOf("month");
        }

        if (modifier === "start of year") {
            return state.startOf("year");
        }

        if (modifier === "start of day") {
            return state.startOf("day");
        }

        if (modifier.startsWith("weekday")) {
            const weekdayN = parseInt(modifier.split(" ")[1]);
            return state.set("day", weekdayN);
        }

        const endings: dayjs.ManipulateType[] = [
            "days",
            "hours",
            "minutes",
            "seconds",
            "months",
            "years",
        ];
        for (const ending of endings) {
            if (modifier.endsWith(ending)) {
                const value = parseInt(modifier.split(" ")[0]);
                return state.add(value, ending);
            }
        }

        return state;
    }, baseDate);
}

export const DateFunctionsMap: {
    [operator in string]: (...args: any[]) => string | number;
} = {
    date: (timeValue?: any, ...modifiers: any[]) => {
        return computeDate(timeValue, ...modifiers).format("YYYY-MM-DD");
    },
    time: (timeValue?: any, ...modifiers: any[]) => {
        return computeDate(timeValue, ...modifiers).format("HH:MM:ss");
    },
    datetime: (timeValue?: any, ...modifiers: any[]) => {
        return computeDate(timeValue, ...modifiers).format(
            "YYYY-MM-DD HH:MM:ss"
        );
    },
    julianday: (timeValue?: any, ...modifiers: any[]) => {
        throw new Error("Unimplemented function `julianday`");
    },
    unixepoch: (timeValue?: any, ...modifiers: any[]) => {
        return computeDate(timeValue, ...modifiers).unix();
    },
    strftime: (timeValue?: any, ...modifiers: any[]) => {
        throw new Error("Unimplemented function `strftime`");
    },
};

export class QueryPlannerDateFunctions {
    static hasFunction(name: string) {
        return !!DateFunctionsMap[name];
    }

    static executeFunction(name: string, args: any[]) {
        const fn = DateFunctionsMap[name];

        return fn.call(fn, ...args);
    }
}
