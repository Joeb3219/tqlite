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
            const bytes = database.getBytesOnPage(database.header, 22);
            const page = database.loadPage(23, []);
            console.log(page);
            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();
            fs.writeFileSync("/Users/joeb3219/Desktop/pg23_real.bin", bytes);
            fs.writeFileSync("/Users/joeb3219/Desktop/pg23_mine.bin", result);

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
            const bytes = database.getBytesOnPage(database.header, 22);
            const page = database.loadPage(23, []);
            console.log(page);
            const writer = new DatabaseFileBTreePageWriter(database, page);
            const result = writer.getBytesBuffer();
            fs.writeFileSync("/Users/joeb3219/Desktop/pg23_real.bin", bytes);
            fs.writeFileSync("/Users/joeb3219/Desktop/pg23_mine.bin", result);

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
    });
});
