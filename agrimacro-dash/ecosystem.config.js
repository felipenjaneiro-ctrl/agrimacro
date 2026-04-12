module.exports = {
  apps: [{
    name: 'agrimacro-dash',
    script: 'node_modules/next/dist/bin/next',
    args: 'dev --port 3000',
    cwd: 'C:\\Users\\felip\\OneDrive\\Área de Trabalho\\agrimacro\\agrimacro-dash',
    watch: false,
    autorestart: true,
    max_restarts: 5,
    windowsHide: true,
    interpreter: 'node',
    env: {
      NODE_ENV: 'development',
      PORT: 3000
    }
  }]
}
