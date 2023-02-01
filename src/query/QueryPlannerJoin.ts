import { ResultSet } from "./QueryPlanner.types";

type EvaluationCriterion = (proposedRow: any) => boolean;

export class QueryPlannerJoin {
    static innerJoin(
        setA: ResultSet,
        setB: ResultSet,
        evaluateCriterion: EvaluationCriterion
    ): ResultSet {
        const data: ResultSet = [];

        if (setA.length === 0) {
            return setB;
        }

        if (setB.length === 0) {
            return setA;
        }

        for (const rowA of setA) {
            for (const rowB of setB) {
                const proposedRow = { ...rowA, ...rowB };
                if (evaluateCriterion(proposedRow)) {
                    data.push(proposedRow);
                }
            }
        }

        return data;
    }

    static fullJoin(
        setA: ResultSet,
        setB: ResultSet,
        evaluateCriterion: EvaluationCriterion
    ): ResultSet {
        const data: ResultSet = [];

        const insertedRowBsIndices: string[] = [];
        for (const rowA of setA) {
            let didRowInsert: boolean = false;
            for (const rowBIdx in setB) {
                const rowB = setB[rowBIdx];
                const proposedRow = { ...rowA, ...rowB };
                if (evaluateCriterion(proposedRow)) {
                    data.push(proposedRow);
                    insertedRowBsIndices.push(rowBIdx);
                    didRowInsert = true;
                }
            }

            if (!didRowInsert) {
                data.push(rowA);
            }
        }

        for (const rowBIdx in setB) {
            if (!insertedRowBsIndices.includes(rowBIdx)) {
                data.push(setB[rowBIdx]);
            }
        }

        return data;
    }

    // TODO: implement cross-join query-optimizer
    static crossJoin(
        setA: ResultSet,
        setB: ResultSet,
        evaluateCriterion: EvaluationCriterion
    ): ResultSet {
        return QueryPlannerJoin.innerJoin(setA, setB, evaluateCriterion);
    }

    static leftJoin(
        setA: ResultSet,
        setB: ResultSet,
        evaluateCriterion: EvaluationCriterion
    ): ResultSet {
        const data: ResultSet = [];

        for (const rowA of setA) {
            const rowB = setB.find((rowB) =>
                evaluateCriterion({ ...rowA, ...rowB })
            );
            data.push({ ...rowA, ...rowB });
        }

        return data;
    }

    static rightJoin(
        setA: ResultSet,
        setB: ResultSet,
        evaluateCriterion: EvaluationCriterion
    ): ResultSet {
        return QueryPlannerJoin.leftJoin(setB, setA, evaluateCriterion);
    }
}
