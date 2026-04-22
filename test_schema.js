import { parquetMetadata } from 'hyparquet';
import fs from 'fs';

const fsData = fs.readFileSync('dataset.parquet');
const buffer = new Uint8Array(fsData).buffer;

const md = parquetMetadata(buffer);
console.log(JSON.stringify(md.schema, null, 2));
