import {
    BTreePage,
    DatabaseFile,
    MasterSchemaEntry,
    QueryPlanner,
} from "@joeb3219/squeel";
import {
    Button,
    Divider,
    Grid,
    MenuItem,
    Select,
    Tab,
    Tabs,
    Typography,
} from "@material-ui/core";
import DownloadIcon from "@material-ui/icons/GetApp";
import UploadIcon from "@material-ui/icons/Publish";
import React from "react";
import BaseHexEditor from "react-hex-editor";
import "./App.css";

import _ from "lodash";
import DataGrid from "react-data-grid";
import "react-data-grid/lib/styles.css";

const Header: React.FC = () => {
    return (
        <Grid
            item
            container
            direction={"row"}
            style={{
                padding: 16,
                width: "100%",
                backgroundColor: "#5D7681",
                height: 56,
            }}
        >
            <Grid item>
                <Typography variant={"h6"} color={"primary"}>
                    Squeel, the pure Typescript SQLite Client
                </Typography>
            </Grid>
            <Grid item xs />
            <Grid item style={{ marginTop: 4 }}>
                <Typography variant={"body1"} color={"secondary"}>
                    By Joseph A. Boyle
                </Typography>
            </Grid>
        </Grid>
    );
};

const TableViewer: React.VFC<{ database: DatabaseFile }> = ({ database }) => {
    const tables = React.useMemo(() => {
        const schema = database.readMasterSchema();
        return schema.filter((s) => s.type === "table");
    }, [database]);

    const [selectedTable, setSelectedTable] = React.useState<
        MasterSchemaEntry | undefined
    >(undefined);

    const selectedTableRows = React.useMemo(() => {
        return selectedTable ? database.getRows(selectedTable.name) : [];
    }, [database, selectedTable]);

    return (
        <Grid container direction={"column"}>
            <Grid item>
                <Select
                    onChange={(event) =>
                        setSelectedTable(
                            tables[
                                typeof event.target.value === "number"
                                    ? event.target.value
                                    : -1
                            ]
                        )
                    }
                >
                    {tables.map((table, idx) => (
                        <MenuItem value={idx}>{table.tbl_name}</MenuItem>
                    ))}
                </Select>
            </Grid>
            <Grid item>
                <DataGrid
                    rows={selectedTableRows}
                    columns={
                        selectedTable?.tableDefinition?.columns?.map((c) => ({
                            key: c.name,
                            name: `${c.name}`,
                            resizable: true,
                        })) ?? []
                    }
                    style={{
                        height: "86vh",
                    }}
                />
            </Grid>
        </Grid>
    );
};

const IndexViewer: React.VFC<{ database: DatabaseFile }> = ({ database }) => {
    const indices = React.useMemo(() => {
        const schema = database.readMasterSchema();
        return schema.filter((s) => s.type === "index");
    }, [database]);

    const [selectedIndex, setSelectedIndex] = React.useState<
        MasterSchemaEntry | undefined
    >(undefined);

    const selectedIndexRows = React.useMemo(() => {
        return selectedIndex ? database.getRows(selectedIndex.name) : [];
    }, [database, selectedIndex]);

    return (
        <Grid container direction={"column"}>
            <Grid item>
                <Select
                    onChange={(event) =>
                        setSelectedIndex(
                            indices[
                                typeof event.target.value === "number"
                                    ? event.target.value
                                    : -1
                            ]
                        )
                    }
                >
                    {indices.map((index, idx) => (
                        <MenuItem value={idx}>{index.name}</MenuItem>
                    ))}
                </Select>
            </Grid>
            <Grid item>
                <DataGrid
                    rows={selectedIndexRows}
                    columns={
                        selectedIndex?.indexDefinition?.columns.map((c) => ({
                            key: c,
                            name: c,
                        })) ?? []
                    }
                    style={{
                        height: "86vh",
                    }}
                />
            </Grid>
        </Grid>
    );
};

const QueryViewer: React.VFC<{ database: DatabaseFile }> = ({ database }) => {
    const results = React.useMemo(() => {
        const query = new QueryPlanner(database, {
            from: {
                type: "join",
                leftTable: "mods",
                rightTable: "knex_migrations",
                on: {
                    a: {
                        type: "column",
                        table: "mods",
                        column: "isNew",
                    },
                    b: {
                        type: "column",
                        table: "knex_migrations",
                        column: "batch",
                    },
                    comparator: "<",
                },
            },
            what: [],
            where: [],
            sort: {
                table: "mods",
                column: "size",
                direction: "ascending",
            },
        });

        return query.execute();
    }, [database]);

    return (
        <DataGrid
            rows={results}
            columns={Object.keys(results[0]).map((r) => ({
                key: r,
                name: r,
                resizable: true
            }))}
            style={{
                height: "86vh",
            }}
            enableVirtualization
        />
    );
};

const PageViewer: React.VFC<{ database: DatabaseFile }> = ({ database }) => {
    const [selectedPage, setSelectedPage] = React.useState<
        BTreePage | undefined
    >(undefined);
    const pageKeys = _.range(0, database.header.databaseFileSizeInPages);

    return (
        <Grid container direction={"column"}>
            <Grid item>
                <Select
                    onChange={(event) => {
                        try {
                            const pageNumber =
                                typeof event.target.value === "number"
                                    ? event.target.value
                                    : -1;
                            const page = database.loadPage(pageNumber, []);
                            setSelectedPage(page);
                        } catch (err) {
                            console.error("Failed to parse page", err);
                            setSelectedPage(undefined);
                        }
                    }}
                >
                    {pageKeys.map((pageNumber) => (
                        <MenuItem value={pageNumber + 1}>
                            {pageNumber + 1}
                        </MenuItem>
                    ))}
                </Select>
            </Grid>
            {selectedPage && (
                <Grid item>{JSON.stringify(selectedPage, null, 2)}</Grid>
            )}
        </Grid>
    );
};

const SchemaViewer: React.VFC<{ database: DatabaseFile }> = ({ database }) => {
    const schema = React.useMemo(() => {
        console.log(database);
        return database.readMasterSchema();
    }, [database]);

    React.useEffect(() => {
        console.log(schema);
    }, [schema]);

    const columns = [
        { key: "type", name: "Type" },
        { key: "name", name: "Name" },
        { key: "tbl_name", name: "Table Name" },
        { key: "rootpage", name: "Root Page" },
        { key: "sql", name: "SQL" },
    ];

    return (
        <DataGrid
            rows={schema}
            columns={columns}
            style={{
                height: "86vh",
            }}
        />
    );
};

interface BodyPanelProps {
    database: DatabaseFile | undefined;
    setDatabase: (db: DatabaseFile | undefined) => void;
    style?: React.CSSProperties;
}

const BodyPanel: React.FC<BodyPanelProps> = (props) => {
    const [value, setValue] = React.useState(0);

    return (
        <Grid
            item
            container
            direction={"column"}
            xs={10}
            style={{ padding: "16px", height: "100%", width: "100%" }}
            spacing={1}
        >
            {props.database && (
                <>
                    <Grid item>
                        <Tabs
                            value={value}
                            onChange={(_event, val) =>
                                typeof val === "number" && setValue(val)
                            }
                        >
                            <Tab label={"Schema"} />
                            <Tab label={"Tables"} />
                            <Tab label={"Views"} />
                            <Tab label={"Pages"} />
                            <Tab label={"Query"} />
                        </Tabs>
                    </Grid>
                    <Grid item>
                        <>
                            {value === 0 && (
                                <SchemaViewer database={props.database} />
                            )}
                            {value === 1 && (
                                <TableViewer database={props.database} />
                            )}
                            {value === 2 && (
                                <IndexViewer database={props.database} />
                            )}
                            {value === 3 && (
                                <PageViewer database={props.database} />
                            )}
                            {value === 4 && (
                                <QueryViewer database={props.database} />
                            )}
                        </>
                    </Grid>
                </>
            )}
        </Grid>
    );
};

function useBasicConversion(lines: string[]) {
    const [bytes, setBytes] = React.useState<number[][]>([]);

    React.useEffect(() => {}, [lines]);

    return bytes;
}

function useSaveWaveFile(lines: string[]) {
    return React.useCallback((shouldAutoRun: boolean) => {}, [lines]);
}

interface RightPanelProps {
    database: DatabaseFile | undefined;
    setDatabase: (db: DatabaseFile | undefined) => void;
    style?: React.CSSProperties;
}

const RightPanel: React.FC<RightPanelProps> = ({ database, setDatabase }) => {
    const [error, setError] = React.useState<boolean>(false);
    const ref = React.createRef<typeof BaseHexEditor>();

    return (
        <Grid
            xs={2}
            item
            style={{
                height: "100%",
                width: "100%",
                backgroundColor: "#37464C",
            }}
        >
            <Grid
                item
                container
                direction={"column"}
                style={{ height: "100%", width: "100%", padding: "16px" }}
                spacing={1}
            >
                <Grid item>
                    <>
                        <Grid container direction={"row"} spacing={1}>
                            <Grid item>
                                <Button
                                    variant={"contained"}
                                    color={"primary"}
                                    onClick={() => {
                                        // TODO: re-add download
                                    }}
                                    startIcon={<DownloadIcon />}
                                >
                                    Save
                                </Button>
                            </Grid>
                            <Grid item>
                                <Button
                                    variant={"contained"}
                                    component={"label"}
                                    color={"primary"}
                                    startIcon={<UploadIcon />}
                                >
                                    Upload
                                    <input
                                        type="file"
                                        hidden
                                        onChange={(event) => {
                                            const fileReader = new FileReader();
                                            const blob =
                                                event.target.files?.[0];

                                            if (!blob) {
                                                // TOOD: snackbar.
                                                console.error("No file read.");
                                                return;
                                            }

                                            fileReader.readAsArrayBuffer(blob);
                                            fileReader.onload = (e) => {
                                                const result = e.target?.result;
                                                if (
                                                    !result ||
                                                    typeof result === "string"
                                                ) {
                                                    return;
                                                }

                                                const db = new DatabaseFile(
                                                    result
                                                );
                                                setDatabase(db);
                                            };
                                        }}
                                    />
                                </Button>
                            </Grid>
                        </Grid>
                    </>
                    <Grid item style={{ marginTop: 8 }}>
                        <Typography variant={"h6"} color={"primary"}>
                            SQLite
                        </Typography>
                        <Typography variant={"body2"} color={"primary"}>
                            Upload a SQLite3 database to begin exploring it.
                        </Typography>
                    </Grid>
                    <Grid item></Grid>
                    <Grid item style={{ marginTop: 8 }}>
                        <Divider
                            variant={"middle"}
                            style={{ color: "#e1e1e1" }}
                        />
                    </Grid>
                </Grid>
            </Grid>
        </Grid>
    );
};

function App() {
    const [database, setDatabase] = React.useState<DatabaseFile | undefined>(
        undefined
    );

    return (
        <Grid
            container
            direction={"column"}
            style={{
                backgroundColor: "#485B63",
                height: "100%",
                width: "100vw",
                margin: 0,
                padding: 0,
                minHeight: "100vh",
            }}
        >
            <Header />
            <Grid item container direction={"row"} style={{ height: "100vh" }}>
                <BodyPanel database={database} setDatabase={setDatabase} />
                <RightPanel database={database} setDatabase={setDatabase} />
            </Grid>
        </Grid>
    );
}

export default App;
