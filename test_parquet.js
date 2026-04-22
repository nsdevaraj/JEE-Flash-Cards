import { parquetRead } from 'hyparquet';
import fs from 'fs';

const fsData = fs.readFileSync('dataset.parquet');
const buffer = new Uint8Array(fsData).buffer;

const data = [];
await parquetRead({
    file: buffer,
    columns: ['images'],
    parsers: {
      stringFromBytes: (bytes) => {
        // Just return the raw bytes instead of a string, or return an object
        return { raw: Array.from(bytes) };
      }
    },
    onComplete: (rows) => {
        const img = rows[0][0][0]; // first row, first column, first image
        console.log("img keys:", Object.keys(img || {}));
        if (img && img.bytes) {
            console.log("bytes type:", typeof img.bytes, "is array:", Array.isArray(img.bytes), "length:", img.bytes.length);
            if (typeof img.bytes === 'string') {
                console.log("bytes start:", img.bytes.substring(0, 20));
            } else if (img.bytes instanceof Uint8Array) {
                console.log("bytes values:", img.bytes.subarray(0, 10));
            }
        }
    }
});
