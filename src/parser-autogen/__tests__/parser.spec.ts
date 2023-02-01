import { parse } from '../parser';
describe('Parser', () => {
    it('should work', () => {
        const parsed = parse('select *, baz.* FROM bar ba LEFT JOIN baz a');
        expect(parsed).toEqual({});
    })
})