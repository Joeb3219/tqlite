import {
    ASTKinds,
    select_from,
    select_from_1,
    select_from_table_or_subquery,
    select_ordering_term_list,
    select_result_column,
    select_result_column_list,
} from "../parser-autogen/parser";

export type FlattenedSelectFrom = select_from_1 | select_from_table_or_subquery;

export class ASTUtil {
    static flattenSelectRealColumnList(
        list: select_result_column_list
    ): select_result_column[] {
        return [
            list.select_result_column,
            ...list.other_result_columns.map((l) => l.select_result_column),
        ];
    }

    static flattenSelectFrom(from: select_from): FlattenedSelectFrom[] {
        switch (from.kind) {
            case ASTKinds.select_from_1:
                return [from];
            case ASTKinds.select_from_2:
                return [
                    from.table_or_subquery.table_or_subquery,
                    ...from.table_or_subquery.other_table_or_subqueries.map(
                        (o) => o.table_or_subquery
                    ),
                ];
        }
    }

    static flattenOrderByTermList(termList: select_ordering_term_list) {
        return [termList.term, ...termList.other_terms.map((t) => t.term)];
    }
}
