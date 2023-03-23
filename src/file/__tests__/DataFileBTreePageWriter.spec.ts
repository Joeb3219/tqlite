import fs from "fs";
import _ from "lodash";
import { DatabaseFile } from "../DatabaseFile";
import { DatabaseFileBTreePageUtil } from "../DatabaseFileBTreePage";
import { DatabaseFileBTreePageWriter } from "../DatabaseFileBTreePageWriter";

describe("DatabaseFileBTreePageWriter", () => {
    describe("numToVarInt", () => {
        it("should return the same results as its inverse", () => {
            const startingValue = 102_403_999;
            const varIntBuffer =
                DatabaseFileBTreePageWriter.numToVarInt(startingValue);
            const result = DatabaseFileBTreePageUtil.readVarInt(
                varIntBuffer,
                0
            );
            expect(result.value).toEqual(startingValue);
            expect(result.length).toEqual(4);
        });

        it("should return the same results as its inverse for 1-byte numbers", () => {
            for (const startingValue of _.range(0, 127)) {
                const varIntBuffer =
                    DatabaseFileBTreePageWriter.numToVarInt(startingValue);
                const result = DatabaseFileBTreePageUtil.readVarInt(
                    varIntBuffer,
                    0
                );
                expect(result.value).toEqual(startingValue);
                expect(result.length).toEqual(1);
            }
        });

        it("should return the same results as its inverse for 2-byte numbers", () => {
            for (const startingValue of _.range(128, 9000)) {
                const varIntBuffer =
                    DatabaseFileBTreePageWriter.numToVarInt(startingValue);
                const result = DatabaseFileBTreePageUtil.readVarInt(
                    varIntBuffer,
                    0
                );
                expect(result.value).toEqual(startingValue);
                expect(result.length).toEqual(2);
            }
        });
    });

    describe("basic writer", () => {
        it("should return the same data when reading a re-written table leaf page", () => {
            const f = fs.readFileSync(
                "/Users/joeb3219/Desktop/other_sample.sqlite"
            );
            const database = new DatabaseFile(f);
            const page = database.loadPage(23, []);

            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();

            const reconverted = DatabaseFileBTreePageUtil.parseBTreePage(
                result,
                22,
                database.header,
                (pageNumber) =>
                    database.getBytesOnPage(database.header, pageNumber - 1),
                []
            );
            expect(reconverted).toEqual(database.loadPage(23, []));
        });

        it("should return the same data when reading a re-written table interior page", () => {
            const f = fs.readFileSync(
                "/Users/joeb3219/Desktop/other_sample.sqlite"
            );
            const database = new DatabaseFile(f);
            const page = database.loadPage(19, []);

            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();

            const reconverted = DatabaseFileBTreePageUtil.parseBTreePage(
                result,
                18,
                database.header,
                (pageNumber) =>
                    database.getBytesOnPage(database.header, pageNumber - 1),
                []
            );
            expect(reconverted).toEqual(database.loadPage(19, []));
        });

        it("should return the same data when reading a re-written index interior page", () => {
            const f = fs.readFileSync(
                "/Users/joeb3219/Desktop/other_sample.sqlite"
            );
            const database = new DatabaseFile(f);
            const page = database.loadPage(26, []);

            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();

            const reconverted = DatabaseFileBTreePageUtil.parseBTreePage(
                result,
                25,
                database.header,
                (pageNumber) =>
                    database.getBytesOnPage(database.header, pageNumber - 1),
                []
            );
            expect(reconverted).toEqual(database.loadPage(26, []));
        });

        it("should return the same data when reading a re-written index leaf page", () => {
            const f = fs.readFileSync(
                "/Users/joeb3219/Desktop/other_sample.sqlite"
            );
            const database = new DatabaseFile(f);
            const page = database.loadPage(20, []);

            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();

            const reconverted = DatabaseFileBTreePageUtil.parseBTreePage(
                result,
                19,
                database.header,
                (pageNumber) =>
                    database.getBytesOnPage(database.header, pageNumber - 1),
                []
            );
            expect(reconverted).toEqual(database.loadPage(20, []));
        });

        it("should return the same data when reading the first page", () => {
            const f = fs.readFileSync(
                "/Users/joeb3219/Desktop/other_sample.sqlite"
            );
            const database = new DatabaseFile(f);
            const page = database.loadPage(1, []);

            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();

            const reconverted = DatabaseFileBTreePageUtil.parseBTreePage(
                result,
                0,
                database.header,
                (pageNumber) =>
                    database.getBytesOnPage(database.header, pageNumber - 1),
                []
            );
            expect(reconverted).toEqual(database.loadPage(1, []));
        });
    });
});
