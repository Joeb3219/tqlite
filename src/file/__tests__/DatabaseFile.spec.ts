import { DatabaseFile } from "../DatabaseFile";

describe("DatabaseFile", () => {
    it("should work", () => {
        const database = new DatabaseFile(
            "/Users/joeb3219/Desktop/other_sample.sqlite"
        );
        const res = JSON.stringify(database.readDatabase(), null, 2);
        console.log(res);
    });
});
