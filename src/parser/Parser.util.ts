import { ParseResult } from "../parser-autogen/parser";

export class ParserUtil {
    static reportParseError(sql: string, parseResult: ParseResult) {
        const error = parseResult.errs[0];
        if (!error) {
            return;
        }

        console.error(`Failed to parse statement`, {
            originalStatement: sql,
            errorEncouteredAt: sql.substring(error.pos.overallPos),
            position: error.pos,
            expected: error.expmatches.map(
                (match) =>
                    `${
                        match.kind === "RegexMatch"
                            ? `${match.negated ? "Not " : ""}${match.literal}`
                            : `${match.negated ? "Not " : ""} End Of String`
                    }`
            ),
        });
        throw new Error(`Failed to parse SQL statement`);
    }
}
