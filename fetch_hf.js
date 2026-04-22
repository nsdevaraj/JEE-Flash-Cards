import https from 'https';

const options = {
  hostname: 'huggingface.co',
  path: '/api/datasets/ShadenA/MathNet/tree/main/data/all',
  method: 'GET',
  headers: { 'User-Agent': 'Node.js' }
};

const req = https.request(options, (res) => {
  let data = '';
  res.on('data', (d) => {
    data += d;
  });
  res.on('end', () => {
    console.log(data);
  });
});

req.on('error', (e) => {
  console.error(e);
});
req.end();
