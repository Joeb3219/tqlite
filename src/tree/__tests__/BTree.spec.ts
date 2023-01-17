import { BTree } from "../BTree";

describe("BTree", () => {
    it("should correctly insert the nodes", () => {
        const btree = new BTree<{ a: number; b: string }>(["a"]);
        btree.addNode({ a: 1, b: "hello" });
        btree.addNode({ a: 10, b: "hello 1" });
        btree.addNode({ a: 1100, b: "hello 2" });
        btree.addNode({ a: -50, b: "hello 3" });
        btree.addNode({ a: 55, b: "hello 4" });
        btree.addNode({ a: 99, b: "hello 5" });
        btree.addNode({ a: -4, b: "hello 6" });
        btree.addNode({ a: -5, b: "hello 7" });
        expect(btree.nodes).toMatchInlineSnapshot(`
            {
              "0": {
                "a": 1,
                "b": "hello",
              },
              "1": {
                "a": -50,
                "b": "hello 3",
              },
              "13": {
                "a": 55,
                "b": "hello 4",
              },
              "2": {
                "a": 10,
                "b": "hello 1",
              },
              "28": {
                "a": 99,
                "b": "hello 5",
              },
              "4": {
                "a": -4,
                "b": "hello 6",
              },
              "6": {
                "a": 1100,
                "b": "hello 2",
              },
              "9": {
                "a": -5,
                "b": "hello 7",
              },
            }
        `);

        expect(btree.sort(true)).toMatchInlineSnapshot(`
            [
              {
                "a": -50,
                "b": "hello 3",
              },
              {
                "a": -5,
                "b": "hello 7",
              },
              {
                "a": -4,
                "b": "hello 6",
              },
              {
                "a": 1,
                "b": "hello",
              },
              {
                "a": 10,
                "b": "hello 1",
              },
              {
                "a": 55,
                "b": "hello 4",
              },
              {
                "a": 99,
                "b": "hello 5",
              },
              {
                "a": 1100,
                "b": "hello 2",
              },
            ]
        `);

        expect(btree.sort(false)).toMatchInlineSnapshot(`
            [
              {
                "a": 1100,
                "b": "hello 2",
              },
              {
                "a": 99,
                "b": "hello 5",
              },
              {
                "a": 55,
                "b": "hello 4",
              },
              {
                "a": 10,
                "b": "hello 1",
              },
              {
                "a": 1,
                "b": "hello",
              },
              {
                "a": -4,
                "b": "hello 6",
              },
              {
                "a": -5,
                "b": "hello 7",
              },
              {
                "a": -50,
                "b": "hello 3",
              },
            ]
        `);
    });

    it("should correctly insert the nodes when comparing column b", () => {
        const btree = new BTree<{ a: number; b: string }>(["b"]);
        btree.addNode({ a: 1, b: "hello" });
        btree.addNode({ a: 10, b: "hello 1" });
        btree.addNode({ a: 1100, b: "hello 2" });
        btree.addNode({ a: -50, b: "hello 3" });
        btree.addNode({ a: 55, b: "hello 4" });
        btree.addNode({ a: 99, b: "hello 5" });
        btree.addNode({ a: -4, b: "hello 6" });
        btree.addNode({ a: -5, b: "hello 7" });
        expect(btree.nodes).toMatchInlineSnapshot(`
            {
              "0": {
                "a": 1,
                "b": "hello",
              },
              "126": {
                "a": -4,
                "b": "hello 6",
              },
              "14": {
                "a": -50,
                "b": "hello 3",
              },
              "2": {
                "a": 10,
                "b": "hello 1",
              },
              "254": {
                "a": -5,
                "b": "hello 7",
              },
              "30": {
                "a": 55,
                "b": "hello 4",
              },
              "6": {
                "a": 1100,
                "b": "hello 2",
              },
              "62": {
                "a": 99,
                "b": "hello 5",
              },
            }
        `);

        expect(btree.sort(true)).toMatchInlineSnapshot(`
            [
              {
                "a": 1,
                "b": "hello",
              },
              {
                "a": 10,
                "b": "hello 1",
              },
              {
                "a": 1100,
                "b": "hello 2",
              },
              {
                "a": -50,
                "b": "hello 3",
              },
              {
                "a": 55,
                "b": "hello 4",
              },
              {
                "a": 99,
                "b": "hello 5",
              },
              {
                "a": -4,
                "b": "hello 6",
              },
              {
                "a": -5,
                "b": "hello 7",
              },
            ]
        `);

        expect(btree.sort(false)).toMatchInlineSnapshot(`
            [
              {
                "a": -5,
                "b": "hello 7",
              },
              {
                "a": -4,
                "b": "hello 6",
              },
              {
                "a": 99,
                "b": "hello 5",
              },
              {
                "a": 55,
                "b": "hello 4",
              },
              {
                "a": -50,
                "b": "hello 3",
              },
              {
                "a": 1100,
                "b": "hello 2",
              },
              {
                "a": 10,
                "b": "hello 1",
              },
              {
                "a": 1,
                "b": "hello",
              },
            ]
        `);
    });
});
