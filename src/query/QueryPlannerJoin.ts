import { select_from_join } from '../parser-autogen/parser';
import { ResultSet } from './QueryPlanner.types';

type EvaluationCriterion = (a: any, b: any) => boolean;

export class QueryPlannerJoin {
    static innerJoin(setA: ResultSet, setB: ResultSet, evaluateCriterion: EvaluationCriterion): ResultSet {
        const data: ResultSet = [];

        for (const rowA of setA) {
            for (const rowB of setB) {
                if (evaluateCriterion(rowA, rowB)) {
                    data.push({ ...rowA, ...rowB });
                }
            }    
        }

        return data
    }

    static leftJoin(setA: ResultSet, setB: ResultSet, evaluateCriterion: EvaluationCriterion): ResultSet {
        const data: ResultSet = [];

        for (const rowA of setA) {
            const rowB = setB.find(rowB => evaluateCriterion(rowA, rowB));
            data.push({ ...rowA, ...rowB });
        }

        return data;
    }

    static rightJoin(setA: ResultSet, setB: ResultSet, evaluateCriterion: EvaluationCriterion): ResultSet {
        return QueryPlannerJoin.leftJoin(setB, setA, evaluateCriterion);
    }
}