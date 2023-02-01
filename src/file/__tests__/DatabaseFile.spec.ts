import { DatabaseFile } from "../DatabaseFile";

import fs from "fs";
import { parse } from "../../parser-autogen/parser";

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

    it("should foo", () => {
        const parsed = parse(
            "SELECT a.* FROM alpha a WHERE a.foo > 5 AND a.bar <= 10"
        );
        expect(parsed).toEqual({});
    });
});
