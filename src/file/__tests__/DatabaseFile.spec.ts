import { DatabaseFile } from "../DatabaseFile";

import fs from "fs";

describe("DatabaseFile", () => {
    it("should work", () => {
        const f = fs.readFileSync(
            "/Users/joeb3219/Desktop/other_sample.sqlite"
        );
        const database = new DatabaseFile(f);
        const res = JSON.stringify(database.schema, null, 2);
        fs.writeFileSync("/Users/joeb3219/Desktop/out.json", res);
        // console.log(res);
    });
});
