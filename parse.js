import { parquetRead } from 'hyparquet';
import fs from 'fs';
import https from 'https';
import path from 'path';

const getTree = async (treePath) => {
  const res = await fetch('https://huggingface.co/api/datasets/ShadenA/MathNet/tree/refs%2Fconvert%2Fparquet/' + treePath);
  return await res.json();
};

const downloadFile = async (url, dest) => {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const handleResponse = (res) => {
      if (res.statusCode === 302 || res.statusCode === 301) {
        https.get(res.headers.location, handleResponse);
      } else {
        res.pipe(file);
        file.on("finish", () => {
          file.close();
          resolve();
        });
      }
    };
    https.get(url, handleResponse).on('error', reject);
  });
};

async function main() {
  console.log("Discovering parquet files...");
  const root = await getTree('');
  const parquetUrls = [];
  
  for (const item of root) {
    if (item.type === 'directory') {
      if (item.path === 'all') continue; // skip 'all' because it overlaps

      const sub = await getTree(item.path + '/train');
      for (const f of sub) {
        if (f.path.endsWith('.parquet')) {
          parquetUrls.push({
            name: f.path.split('/').pop() + '_' + item.path + '.parquet',
            url: `https://huggingface.co/datasets/ShadenA/MathNet/resolve/refs%2Fconvert%2Fparquet/${f.path}?download=true`
          });
        }
      }
    }
  }

  console.log(`Found ${parquetUrls.length} parquet files. Downloading...`);
  if (!fs.existsSync('./tmp_parquets')) fs.mkdirSync('./tmp_parquets');

  const parsed = [];
  const counts = { 'Algebra': 0, 'Geometry': 0, 'Discrete Mathematics': 0, 'Number Theory': 0 };
  const MAX_QUESTIONS_PER_SUBJECT = 8000;

  for (let i = 0; i < parquetUrls.length; i++) {
   const isFull = Object.values(counts).reduce((a, b) => a + b, 0) >= MAX_QUESTIONS_PER_SUBJECT * 4;
   if (isFull) {
       console.log("Reached limit. Stopping.");
       //break; // we won't break for now, let's parse more
   }
    const p = parquetUrls[i];
    const dest = path.join('./tmp_parquets', p.name);
    console.log(`Downloading ${i+1}/${parquetUrls.length}: ${p.name}`);
    await downloadFile(p.url, dest);
    
    const fileBuffer = fs.readFileSync(dest);
    
    await parquetRead({
      file: fileBuffer.buffer,
      rowFormat: 'object',
      parsers: {
          stringFromBytes: (bytes) => {
              if (!bytes) return null;
              if (bytes.length > 8 && bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47) {
                  return { isImage: true, ext: 'png', b64: Buffer.from(bytes).toString('base64') };
              }
              if (bytes.length > 2 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
                  return { isImage: true, ext: 'jpeg', b64: Buffer.from(bytes).toString('base64') };
              }
              return new TextDecoder().decode(bytes);
          }
      },
      onComplete: (data) => {
        for (let j = 0; j < data.length; j++) {
           let row = data[j];
           const rawTopics = row.topics || row.topics_flat || [];
           const topics = Array.isArray(rawTopics) ? rawTopics : [rawTopics];
           let problem = Array.isArray(row.problem_markdown) ? row.problem_markdown.join('\n') : String(row.problem_markdown || row.original_problem_markdown || '');
           let answerList = row.final_answer || [];
           let answer = Array.isArray(answerList) ? answerList.join('\n') : String(answerList);
           if (!answer || answer.length < 2 || answer === "undefined" || answer === "null") answer = "Detailed Solution Provided";
           const solutionList = row.solutions_markdown || [];
           let solution = Array.isArray(solutionList) ? solutionList.join('\n') : String(solutionList);
           
           const images = row.images || [];
           if (Array.isArray(images)) {
               images.forEach(img => {
                   if (img.bytes && img.path) {
                       let b64 = '';
                       let ext = 'png';
                       if (img.bytes.isImage) {
                           b64 = img.bytes.b64;
                           ext = img.bytes.ext;
                       } else {
                           b64 = Buffer.from(img.bytes).toString('base64');
                       }
                       const mdImage = `\n![img](data:image/${ext};base64,${b64})\n`;
                       const nameOnly = img.path.split('/').pop();
                       
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
           const ts = JSON.stringify(topics).toLowerCase();
           if (ts.includes('algebra')) subject = "Algebra";
           else if (ts.includes('geometry')) subject = "Geometry";
           else if (ts.includes('discrete')) subject = "Discrete Mathematics";
           else if (ts.includes('number theory')) subject = "Number Theory";
           
           if (subject === "Other") {
             subject = "Algebra"; // Default category
           }

           if (!counts[subject]) counts[subject] = 0;
           if (counts[subject] >= MAX_QUESTIONS_PER_SUBJECT) continue; 

           counts[subject]++;
           parsed.push({
               id: parsed.length + 1,
               subject: subject,
               question: problem,
               options: [],
               answer: answer,
               solution: solution
           });
        }
      }
    });

    fs.unlinkSync(dest);
  }

  console.log("Saving dataset json...");
  fs.writeFileSync('public/mathnet.json', JSON.stringify(parsed, null, 2));
  console.log(`Wrote public/mathnet.json with length ${parsed.length}`);
  console.log("Counts:", counts);
}

main().catch(console.error);
