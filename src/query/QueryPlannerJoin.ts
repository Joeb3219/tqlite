import { ResultSet } from "./QueryPlanner.types";

type EvaluationCriterion = (proposedRow: any) => boolean;

export class QueryPlannerJoin {
    static innerJoin(
        setA: ResultSet,
        setB: ResultSet,
        evaluateCriterion: EvaluationCriterion
    ): ResultSet {
        const data: ResultSet = [];

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
