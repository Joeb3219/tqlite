import { DatabaseFile } from "../DatabaseFile";

describe("DatabaseFile", () => {
    it("should work", () => {
        const database = new DatabaseFile(
            "/Users/joeb3219/Downloads/sample.sqlite"
        );
        console.log(JSON.stringify(database.readDatabase(), null, 2));
    });
});
