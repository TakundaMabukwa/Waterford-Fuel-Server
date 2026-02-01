const path = require('path');
const fs = require('fs');

console.log('Current working directory:', process.cwd());
console.log('__dirname:', __dirname);

const logoPath1 = path.join(process.cwd(), 'assets', 'logo.png');
const logoPath2 = path.resolve(__dirname, '../../assets/logo.png');
const logoPath3 = path.join(__dirname, '../../assets/logo.png');

console.log('Path 1 (process.cwd):', logoPath1);
console.log('Path 2 (resolve):', logoPath2);
console.log('Path 3 (join):', logoPath3);

console.log('Path 1 exists:', fs.existsSync(logoPath1));
console.log('Path 2 exists:', fs.existsSync(logoPath2));
console.log('Path 3 exists:', fs.existsSync(logoPath3));