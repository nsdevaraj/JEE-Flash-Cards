import { parquetMetadata } from 'hyparquet';
import fs from 'fs';

const fsData = fs.readFileSync('dataset.parquet');
const buffer = new Uint8Array(fsData).buffer;

const md = parquetMetadata(buffer);
console.log("Num rows:", Number(md.num_rows));
console.log("Schema:", md.schema.map(f => f.name));
