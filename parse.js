import { parquetRead } from 'hyparquet';
import fs from 'fs';

async function main() {
  const fileBuffer = fs.readFileSync('dataset.parquet');
  
  await parquetRead({
    file: fileBuffer.buffer,
    parsers: {
        stringFromBytes: (bytes) => {
            if (!bytes) return null;
            // Check for PNG signature
            if (bytes.length > 8 && bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47) {
                return { isImage: true, ext: 'png', b64: Buffer.from(bytes).toString('base64') };
            }
            // Check for JPEG signature
            if (bytes.length > 2 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
                return { isImage: true, ext: 'jpeg', b64: Buffer.from(bytes).toString('base64') };
            }
            // Otherwise, decode as UTF-8
            return new TextDecoder().decode(bytes);
        }
    },
    onComplete: (data) => {
      const parsed = [];
      const MAX_QUESTIONS_PER_SUBJECT = 200;
      const counts = { 'Algebra': 0, 'Geometry': 0, 'Discrete Mathematics': 0, 'Number Theory': 0 };

      for (let i = 0; i < data.length; i++) {
         let row = data[i];
         const rawTopics = row[10] || [];
         const topics = Array.isArray(rawTopics) ? rawTopics : [rawTopics];
         let problem = Array.isArray(row[6]) ? row[6].join('\n') : String(row[6]);
         const solutionList = row[7] || [];
         let solution = Array.isArray(solutionList) ? solutionList.join('\n') : String(solutionList);
         
         const images = row[16] || [];
         if (Array.isArray(images)) {
             images.forEach(img => {
                 if (img.bytes && img.path) {
                     let b64 = '';
                     let ext = 'png';
                     if (img.bytes.isImage) {
                         b64 = img.bytes.b64;
                         ext = img.bytes.ext;
                     } else {
                         // Fallback just in case
                         b64 = Buffer.from(img.bytes).toString('base64');
                     }
                     const mdImage = `\n![img](data:image/${ext};base64,${b64})\n`;
                     const nameOnly = img.path.split('/').pop();
                     
                     // safely replace `images/filename` or `filename` in parenthesis
                     if (problem.includes(nameOnly)) {
                         problem = problem.replace(new RegExp(`!\\[.*?\\]\\([^)]*${nameOnly}[^)]*\\)`, 'g'), mdImage);
                     }
                     if (solution.includes(nameOnly)) {
                         solution = solution.replace(new RegExp(`!\\[.*?\\]\\([^)]*${nameOnly}[^)]*\\)`, 'g'), mdImage);
                     }
                     if (!problem.includes(mdImage) && !solution.includes(mdImage)) {
                         problem += mdImage;
                     }
                 }
             });
         }
         
         if (!problem || problem.length < 10) continue;

         let subject = "Other";
         if (topics.some(t => String(t).includes('Algebra'))) subject = "Algebra";
         else if (topics.some(t => String(t).includes('Geometry'))) subject = "Geometry";
         else if (topics.some(t => String(t).includes('Discrete Mathematics'))) subject = "Discrete Mathematics";
         else if (topics.some(t => String(t).includes('Number Theory'))) subject = "Number Theory";

         if (subject === "Other" && topics.length > 0) {
             const parts = String(topics[0]).split(' > ');
             subject = parts.length > 1 ? parts[1] : parts[0];
         }
         if (!counts[subject]) counts[subject] = 0;
         if (counts[subject] >= MAX_QUESTIONS_PER_SUBJECT) continue; // cap per subject

         counts[subject]++;
         parsed.push({
             id: parsed.length + 1,
             subject: subject,
             question: problem,
             options: [],
             answer: "Detailed Solution Provided",
             solution: solution
         });
         
      }
      fs.writeFileSync('public/mathnet.json', JSON.stringify(parsed, null, 2));
      console.log('Wrote public/mathnet.json with length ' + parsed.length);
      console.log('Counts:', counts);
    }
  });
}
main().catch(console.error);
