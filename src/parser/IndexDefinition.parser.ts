type TokenType =
    | "CREATE"
    | "INDEX"
    | "UNIQUE"
    | "ON"
    | "BACKTICK"
    | "("
    | ")"
    | ","
    | "identifier"
    | "number";

const TokenParseMap: Record<TokenType, (str: string) => boolean> = {
    CREATE: (str) => str === "CREATE",
    INDEX: (str) => str === "INDEX",
    UNIQUE: (str) => str === "UNIQUE",
    ON: (str) => str === "ON",
    BACKTICK: (str) => str === `\`` || str === '"',
    "(": (str) => str === "(",
    ")": (str) => str === ")",
    ",": (str) => str === ",",
    identifier: (str) => {
        const match = str.match(/[a-zA-Z_0-9'\[\]]*$/);
        return !!match && match[0].length === str.length;
    },
    number: (str) =>
        /^\s*[+-]?(\d+|\d*\.\d+|\d+\.\d*)([Ee][+-]?\d+)?\s*$/.test(str),
};

type Token<T extends TokenType = TokenType> = {
    type: T;
    lexeme: string;
};

export type IndexDefinition = {
    indexName: string;
    tableName: string;
    unique: boolean;
    columns: string[];
};

export class IndexDefinitionParser {
    tokens: Token[] = [];
    indexDefinition: IndexDefinition | undefined = undefined;
    currentTokenPosition: number = 0;

    constructor(private readonly sql: string) {
        this.setTokens();
        this.parseRoot();
    }

    private isWhitespace(str: string): boolean {
        return str === " " || str === "\t" || str === "\r";
    }

    private isNewLine(str: string): boolean {
        return str === "\n";
    }

    private isEndOfFile(str: string): boolean {
        return str === "\0";
    }

    private readUntilDelimter(startPosition: number): string {
        let endPosition = startPosition + 1;
        while (endPosition < this.sql.length) {
            const endChar = this.sql[endPosition];
            if (
                this.isWhitespace(endChar) ||
                this.isNewLine(endChar) ||
                this.isEndOfFile(endChar)
            ) {
                break;
            }

            endPosition++;
        }

        return this.sql.substring(startPosition, endPosition);
    }

    private findTokenAtIndex(index: number): Token {
        const fullSubstr = this.readUntilDelimter(index);
        for (let i = fullSubstr.length; i > 0; i--) {
            const substr = fullSubstr.substring(0, i);
            const matched = Object.entries(TokenParseMap).find((entry) =>
                entry[1](substr)
            );

            if (matched) {
                return {
                    type: matched[0] as TokenType,
                    lexeme: substr,
                };
            }
        }

        throw new Error(
            `Failed to find token beginning at index ${index}: ${fullSubstr}; ${this.sql}`
        );
    }

    private setTokens() {
        this.tokens = [];

        let currentIndex = 0;
        while (currentIndex < this.sql.length) {
            const char = this.sql[currentIndex];
            if (
                this.isWhitespace(char) ||
                this.isEndOfFile(char) ||
                this.isNewLine(char)
            ) {
                currentIndex++;
                continue;
            }

            const token = this.findTokenAtIndex(currentIndex);
            this.tokens.push(token);
            currentIndex += token.lexeme.length;
        }
    }

    private get token(): Token {
        return this.tokens[this.currentTokenPosition];
    }

    private consumeOfType<T extends TokenType>(type: T): Token<T> {
        const currentToken = this.token;
        if (currentToken.type !== type) {
            throw new Error(
                `Expected to find token of type ${type}, but found ${currentToken.type}`
            );
        }

        this.currentTokenPosition++;
        return currentToken as Token<T>;
    }

    private optionallyConsumeOfType<T extends TokenType>(
        type: T
    ): Token<T> | undefined {
        const currentToken = this.token;
        if (currentToken.type !== type) {
            return undefined;
        }

        this.currentTokenPosition++;
        return currentToken as Token<T>;
    }

    // CREATE TABLE `dlc` (`id` char(36) not null, `name` varchar(255) not null, `dirPath` varchar(255) not null, primary key (`id`))
    private parseColumnDefinitions() {
        let openCount = 0;
        while (this.token) {
            this.optionallyConsumeOfType("BACKTICK");
            const columnName = this.consumeOfType("identifier");
            this.optionallyConsumeOfType("BACKTICK");

            let typeStringParts: string[] = [];
            while (this.token) {
                if (this.token.type === "identifier") {
                    typeStringParts.push(this.token.lexeme);
                    this.consumeOfType("identifier");
                } else if (this.token.type === "(") {
                    typeStringParts.push(this.token.lexeme);
                    openCount++;
                    this.consumeOfType("(");
                } else if (this.token.type === ")") {
                    // We've closed all of the braces, we're finished.
                    if (openCount === 0) {
                        break;
                    } else {
                        typeStringParts.push(this.token.lexeme);
                        openCount--;
                        this.consumeOfType(")");
                    }
                } else {
                    break;
                }
            }

            this.indexDefinition?.columns.push(columnName.lexeme);

            if (this.token?.type !== ",") {
                break;
            }

            this.consumeOfType(",");
        }
    }

    private parseRoot() {
        this.currentTokenPosition = 0;
        this.indexDefinition = undefined;

        this.optionallyConsumeOfType("CREATE");
        const uniqueToken = this.optionallyConsumeOfType("UNIQUE");
        this.consumeOfType("INDEX");
        this.optionallyConsumeOfType("BACKTICK");
        const indexName = this.consumeOfType("identifier");
        this.optionallyConsumeOfType("BACKTICK");
        this.consumeOfType("ON");
        this.optionallyConsumeOfType("BACKTICK");
        const tableName = this.consumeOfType("identifier");
        this.optionallyConsumeOfType("BACKTICK");

        this.indexDefinition = {
            indexName: indexName.lexeme,
            tableName: tableName.lexeme,
            unique: !!uniqueToken,
            columns: [],
        };

        this.optionallyConsumeOfType("BACKTICK");
        this.consumeOfType("(");
        this.parseColumnDefinitions();

        if (!this.indexDefinition.columns.includes("id")) {
            this.indexDefinition.columns.push("id");
        }
    }
}
