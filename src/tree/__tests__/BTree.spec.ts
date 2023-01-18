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
        expect(btree.root).toMatchInlineSnapshot(`
            {
              "data": {
                "a": 1,
                "b": "hello",
              },
              "left": {
                "data": {
                  "a": -50,
                  "b": "hello 3",
                },
                "right": {
                  "data": {
                    "a": -4,
                    "b": "hello 6",
                  },
                  "left": {
                    "data": {
                      "a": -5,
                      "b": "hello 7",
                    },
                  },
                },
              },
              "right": {
                "data": {
                  "a": 10,
                  "b": "hello 1",
                },
                "right": {
                  "data": {
                    "a": 1100,
                    "b": "hello 2",
                  },
                  "left": {
                    "data": {
                      "a": 55,
                      "b": "hello 4",
                    },
                    "right": {
                      "data": {
                        "a": 99,
                        "b": "hello 5",
                      },
                    },
                  },
                },
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
        expect(btree.root).toMatchInlineSnapshot(`
            {
              "data": {
                "a": 1,
                "b": "hello",
              },
              "right": {
                "data": {
                  "a": 10,
                  "b": "hello 1",
                },
                "right": {
                  "data": {
                    "a": 1100,
                    "b": "hello 2",
                  },
                  "right": {
                    "data": {
                      "a": -50,
                      "b": "hello 3",
                    },
                    "right": {
                      "data": {
                        "a": 55,
                        "b": "hello 4",
                      },
                      "right": {
                        "data": {
                          "a": 99,
                          "b": "hello 5",
                        },
                        "right": {
                          "data": {
                            "a": -4,
                            "b": "hello 6",
                          },
                          "right": {
                            "data": {
                              "a": -5,
                              "b": "hello 7",
                            },
                          },
                        },
                      },
                    },
                  },
                },
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
