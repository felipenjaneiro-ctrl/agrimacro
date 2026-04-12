var Service = require('node-windows').Service;
var path = require('path');

var svc = new Service({
  name: 'AgriMacro Dashboard',
  script: path.join(__dirname, 'node_modules', 'next', 'dist', 'bin', 'next')
});

svc.on('uninstall', function() {
  console.log('Service uninstalled.');
});

svc.uninstall();
