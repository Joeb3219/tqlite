// TODO: I want to build this into the library instead of doing this by hand.
// But for now, we will just have to create some types by hand. Kind of stinks, but what can you do for a prototype.

export type Identifier = {
    type: 'identifier';
    variant: 'table';
    name: string;
}

export type DataType = {
    type: 'datatype';
    variant: 'integer' | 'varchar'
}

export type StatementCreateTableColumnDefinition = {
    type: 'definition';
    variant: 'column';
    name: string;

}

export type StatementCreateTable = {
    type: 'statement';
    variant: 'create';
    format: 'table';
    name: Identifier;
    definition: 
}

export type Statement = {} | StatementList;

export type StatementList = {
    type: 'statement';
    variant: 'list';
    statements: Statement[];
}