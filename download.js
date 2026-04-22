import https from 'https';
import fs from 'fs';

const file = fs.createWriteStream("dataset.parquet");
https.get("https://huggingface.co/datasets/ShadenA/MathNet/resolve/main/data/all/train-00000-of-00001.parquet?download=true", function(response) {
  const handleResponse = (res) => {
    if (res.statusCode === 302 || res.statusCode === 301) {
      https.get(res.headers.location, handleResponse);
    } else {
      res.pipe(file);
      file.on("finish", () => {
          file.close();
          console.log("Download Completed");
      });
    }
  };
  handleResponse(response);
});
