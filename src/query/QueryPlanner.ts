import { DatabaseFile } from "../file/DatabaseFile";
import { BTree } from "../tree/BTree";

export type Expression =
    | {
          type: "literal";
          value: number | boolean | null | string;
      }
    | {
          type: "column";
          table: string;
          column: string;
      };

export type Sort = {
    direction: "ascending" | "descending";
    table: string;
    column: string;
};

export type Where = {
    a: Expression;
    b: Expression;
    comparator: "=" | "!=" | ">" | "<";
};

export type Join = {
    type: "join";
    leftTable: string;
    rightTable: string;
    on: Where;
};

export type SingleTable = {
    type: "single";
};

export type Select = {
    what: Expression[];
    from: Join;
    where: Where[];
    sort: Sort;
};

export class QueryPlanner {
    constructor(
        private readonly database: DatabaseFile,
        private readonly query: Select
    ) {}

    evalExpression(exp: Expression, leftRow: any, rightRow: any): any {
        if (exp.type === "literal") {
            return exp.value;
        }

        if (this.query.from.leftTable === exp.table) {
            return leftRow[exp.column];
        }

        if (this.query.from.rightTable === exp.table) {
            return rightRow[exp.column];
        }

        return null;
    }

    getJoinedRows(join: Join): any[] {
        const leftTable = this.database.getTableRowsZipped(join.leftTable);
        const rightTable = this.database.getTableRowsZipped(join.rightTable);

        return leftTable.flatMap((left) => {
            const matches = rightTable.filter((right) => {
                const failsWhere = !this.query.where.every((where) => {
                    const exprA = this.evalExpression(where.a, left, right);
                    const exprB = this.evalExpression(where.b, left, right);

                    return where.comparator === "!="
                        ? exprA != exprB
                        : where.comparator === "="
                        ? exprA == exprB
                        : where.comparator === "<"
                        ? exprA < exprB
                        : where.comparator === ">"
                        ? exprA > exprB
                        : false;
                });

                if (failsWhere) {
                    return false;
                }

                const exprA = this.evalExpression(join.on.a, left, right);
                const exprB = this.evalExpression(join.on.b, left, right);

                return join.on.comparator === "!="
                    ? exprA != exprB
                    : join.on.comparator === "="
                    ? exprA == exprB
                    : join.on.comparator === "<"
                    ? exprA < exprB
                    : join.on.comparator === ">"
                    ? exprA > exprB
                    : false;
            });

            return matches.map((right) => ({ ...left, ...right }));
        });
    }

    execute(): any[] {
        const joinedRows = this.getJoinedRows(this.query.from);

        const btree = new BTree([this.query.sort.column]);
        joinedRows.forEach((row) => btree.addNode(row));

        return btree.sort(this.query.sort.direction === "ascending");
    }
}
