import _ from "lodash";

export class BTree<T extends any> {
    nodes: Record<number, T> = {};
    constructor(private readonly sortKeys: (keyof T)[]) {}

    // Returns -1 if a is less than b, 0 if they're equal, or 1 if a is greater than b.
    compare(a: T, b: T): number {
        const firstNonEqualKey = this.sortKeys.find(
            (k) => a[k as keyof T] !== b[k as keyof T]
        );

        if (!firstNonEqualKey) {
            return 0;
        }

        // TODO: actually correct this, it's currently not to spec.
        return a[firstNonEqualKey as keyof T] < b[firstNonEqualKey as keyof T]
            ? -1
            : 1;
    }

    addNode(node: T) {
        let currentNodeIndex = 1;
        while (true) {
            const currentNode = this.getNode(currentNodeIndex);

            console.log(currentNodeIndex, currentNode);
            // Node is null, this is an insert.
            if (!currentNode) {
                this.setNode(currentNodeIndex, node);
                return;
            }

            // Otherwise we keep moving through the tree to see if we can find our answer.
            const compare = this.compare(node, currentNode);
            if (compare < 0) {
                currentNodeIndex = this.getChildIndex(currentNodeIndex, "left");
            } else {
                currentNodeIndex = this.getChildIndex(
                    currentNodeIndex,
                    "right"
                );
            }
        }
    }

    setNode(idx: number, node: T) {
        this.nodes[idx - 1] = node;
    }

    getNode(idx: number): T | undefined {
        return this.nodes[idx - 1];
    }

    getRootIndex(): number {
        return 1;
    }

    getChildIndex(idx: number, variant: "left" | "right"): number {
        return variant === "left" ? 2 * idx : 2 * idx + 1;
    }

    getParentIndex(idx: number): number {
        return Math.floor(idx / 2);
    }

    private sortInternal(idx: number, isAscending: boolean): T[] {
        const left = this.getNode(this.getChildIndex(idx, "left"));
        const right = this.getNode(this.getChildIndex(idx, "right"));
        const node = this.getNode(idx);

        if (isAscending) {
            return _.flatten([
                ...(left
                    ? this.sortInternal(
                          this.getChildIndex(idx, "left"),
                          isAscending
                      )
                    : []),
                node ?? [],
                ...(right
                    ? this.sortInternal(
                          this.getChildIndex(idx, "right"),
                          isAscending
                      )
                    : []),
            ]);
        }

        return _.flatten([
            ...(right
                ? this.sortInternal(
                      this.getChildIndex(idx, "right"),
                      isAscending
                  )
                : []),
            node ?? [],
            ...(left
                ? this.sortInternal(
                      this.getChildIndex(idx, "left"),
                      isAscending
                  )
                : []),
        ]);
    }

    sort(isAscending: boolean = true): T[] {
        return this.sortInternal(0, isAscending);
    }
}
