export type BTreeNode<T extends any> = {
    data: T;
    left?: BTreeNode<T>;
    right?: BTreeNode<T>;
};

export class BTree<T extends any> {
    root?: BTreeNode<T>;
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
        // No root node, so we'll create one.
        if (!this.root) {
            this.root = {
                data: node,
            };
            return;
        }

        let currentNode = this.root;
        while (currentNode) {
            const compare = this.compare(node, currentNode.data);
            if (compare < 0) {
                // We are less than, but there's already a lesser node -- navigate to it and we'll restart loop.
                if (currentNode.left) {
                    currentNode = currentNode.left;
                } else {
                    // This is the left node since there isn't one yet
                    currentNode.left = {
                        data: node,
                    };

                    // We've inserted -- safe to return.
                    return;
                }
            } else {
                // We are greater than or equal to, but there's already a gte node -- navigate to it and we'll restart loop.
                if (currentNode.right) {
                    currentNode = currentNode.right;
                } else {
                    // This is the right node since there isn't one yet
                    currentNode.right = {
                        data: node,
                    };

                    // We've inserted -- safe to return.
                    return;
                }
            }
        }
    }

    private sortInternal(root: BTreeNode<T>, isAscending: boolean): T[] {
        if (!root) {
            return [];
        }

        if (isAscending) {
            return [
                ...(!!root.left
                    ? this.sortInternal(root.left, isAscending)
                    : []),
                root.data,
                ...(!!root.right
                    ? this.sortInternal(root.right, isAscending)
                    : []),
            ];
        }

        return [
            ...(!!root.right ? this.sortInternal(root.right, isAscending) : []),
            root.data,
            ...(!!root.left ? this.sortInternal(root.left, isAscending) : []),
        ];
    }

    sort(isAscending: boolean = true): T[] {
        if (!this.root) {
            return [];
        }

        return this.sortInternal(this.root, isAscending);
    }
}
