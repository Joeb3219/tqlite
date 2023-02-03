export const MathFunctionsMap: {
    [operator in string]: (...args: number[]) => number;
} = {
    acos: (a: number) => Math.acos(a),
    acosh: (a: number) => Math.acosh(a),
    asin: (a: number) => Math.asin(a),
    asinh: (a: number) => Math.asinh(a),
    atan: (a: number) => Math.atan(a),
    atan2: (a: number, b: number) => Math.atan2(a, b),
    atanh: (a: number) => Math.atanh(a),
    ceil: (a: number) => Math.ceil(a),
    ceiling: (a: number) => Math.ceil(a),
    cos: (a: number) => Math.cos(a),
    cosh: (a: number) => Math.cosh(a),
    degrees: (a: number) => a * (180 / Math.PI),
    exp: (a: number) => Math.exp(a),
    floor: (a: number) => Math.floor(a),
    ln: (a: number) => Math.log(a),
    log: (b: number, x?: number) =>
        x !== undefined ? Math.log(x) / Math.log(b) : Math.log10(b),
    log10: (a: number) => Math.log10(a),
    log2: (a: number) => Math.log2(a),
    mod: (a: number, b: number) => a % b,
    pi: () => Math.PI,
    pow: (a: number, b: number) => a ** b,
    power: (a: number, b: number) => a ** b,
    radians: (a: number) => a * (Math.PI / 180),
    sin: (a: number) => Math.sin(a),
    sinh: (a: number) => Math.sinh(a),
    sqrt: (a: number) => Math.sqrt(a),
    tan: (a: number) => Math.tan(a),
    tanh: (a: number) => Math.tanh(a),
    trunc: (a: number) => Math.trunc(a),
};

export class QueryPlannerMathFunctions {
    static hasFunction(name: string) {
        return !!MathFunctionsMap[name.toLowerCase()];
    }

    static executeFunction(name: string, args: any[]) {
        const fn = MathFunctionsMap[name.toLowerCase()];

        return fn.call(fn, ...args);
    }
}
