import { File } from './File';
import { DatabaseHeader } from './DatabaseFile.types';
import { DatabaseFileHeaderUtil } from './DatabaseFileHeader';

export class DatabaseFile extends File {
    readDatabase() {
        const header = DatabaseFileHeaderUtil.parseHeader(this);
        return { header };
    }

}