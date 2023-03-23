import fs from "fs";
import _ from "lodash";
import { DatabaseFile } from "../DatabaseFile";
import { DatabaseFileBTreePageUtil } from "../DatabaseFileBTreePage";
import { DatabaseFileBTreePageWriter } from "../DatabaseFileBTreePageWriter";

function createWriterResultMatchTest(path: string, testedPageNumber: number) {
    const file = fs.readFileSync(path);
    const database = new DatabaseFile(file);

    // We load, then write the page.
    const expectedPage = database.loadPage(testedPageNumber, []);
    const writer = new DatabaseFileBTreePageWriter(database, expectedPage);
    const { page: resultPage, overflowPages: resultOverflowPages } =
        writer.getBytesBuffer();

    // Then we re-read our result, so we can assert the values we get back match.
    // We don't do a byte-by-byte comparison because different SQLite implementaitons may fill empty space differently.
    const reconverted = DatabaseFileBTreePageUtil.parseBTreePage(
        resultPage,
        testedPageNumber - 1,
        database.header,
        (pageNumber) => {
            // If they requested our page, return the result we computed
            if (pageNumber === testedPageNumber) {
                return resultPage;
            }

            // Otherwise, we find it in our overflow pages
            const overflowPage = resultOverflowPages.find(
                (page) => page.pageNumber === pageNumber
            );
            if (!overflowPage) {
                throw new Error("Failed to find page in overflow pages");
            }

            return overflowPage.page;
        },
        []
    );

    expect(reconverted).toEqual(expectedPage);
}

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
        it("should return the same data when reading a re-written table leaf page", () =>
            createWriterResultMatchTest(
                "/Users/joeb3219/Desktop/other_sample.sqlite",
                23
            ));

        it("should return the same data when reading a re-written table interior page", () =>
            createWriterResultMatchTest(
                "/Users/joeb3219/Desktop/other_sample.sqlite",
                19
            ));

        it("should return the same data when reading a re-written index interior page", () =>
            createWriterResultMatchTest(
                "/Users/joeb3219/Desktop/other_sample.sqlite",
                26
            ));

        it("should return the same data when reading a re-written index leaf page", () =>
            createWriterResultMatchTest(
                "/Users/joeb3219/Desktop/other_sample.sqlite",
                20
            ));

        it("should return the same data when reading the first page", () =>
            createWriterResultMatchTest(
                "/Users/joeb3219/Desktop/other_sample.sqlite",
                1
            ));
    });
});
