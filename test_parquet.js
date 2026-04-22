import { parquetRead } from 'hyparquet';
import fs from 'fs';

const fsData = fs.readFileSync('dataset.parquet');
const buffer = new Uint8Array(fsData).buffer;

const data = [];
await parquetRead({
    file: buffer,
    columns: ['answers_markdown'],
    onComplete: (rows) => {
        console.log("Total rows in parquet file:", rows.length);
        for(let i=0; i<Math.min(5, rows.length); i++){
            console.log("Row", i, "answers:", rows[i][0]);
        }
    }
});
