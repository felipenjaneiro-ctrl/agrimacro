var Service = require('node-windows').Service;
var path = require('path');

var svc = new Service({
  name: 'AgriMacro Dashboard',
  description: 'AgriMacro Intelligence Next.js Dashboard on port 3000',
  script: path.join(__dirname, 'node_modules', 'next', 'dist', 'bin', 'next'),
  scriptOptions: 'dev --port 3000',
  nodeOptions: [],
  workingDirectory: __dirname,
  allowServiceLogon: true
});

svc.on('install', function() {
  console.log('Service installed. Starting...');
  svc.start();
});

svc.on('start', function() {
  console.log('Service started!');
});

svc.on('error', function(err) {
  console.error('Error:', err);
});

svc.on('alreadyinstalled', function() {
  console.log('Already installed. Starting...');
  svc.start();
});

svc.install();
