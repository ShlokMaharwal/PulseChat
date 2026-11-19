const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { mkdir, writeFile } = require('node:fs/promises');
const { resolve } = require('node:path');
const { randomUUID } = require('node:crypto');
const exec = promisify(execFile);
(async () => {
  const id = randomUUID().slice(0, 8);
  const dir = resolve('.cache/diagnostics/io-' + id);
  await mkdir(dir, { recursive: true });
  const volume = 'pulsechat-io-' + id;
  const container = volume + '-probe';
  const code = `const fs=require('fs');const {performance}=require('perf_hooks');const line=JSON.stringify({event:'io_probe',body:'x'.repeat(480)})+'\\n';for(let r=0;r<3;r++){for(const path of (r%2?['/windows','/native']:['/native','/windows'])){const fd=fs.openSync(path+'/probe-'+r+'.jsonl','w');const start=performance.now();for(let i=0;i<300;i++)fs.writeSync(fd,line);fs.closeSync(fd);console.log(JSON.stringify({path,repeat:r,writes:300,bytesPerWrite:Buffer.byteLength(line),elapsedMs:performance.now()-start}));}}`;
  await exec('docker', ['volume','create','--label','pulsechat.ioProbe='+id,volume], {timeout:30000});
  let output;
  try {
    output = await exec('docker', ['run','--rm','--name',container,'--user','0:0','-v',volume+':/native','-v',dir+':/windows','pulsechat-gateway:local','node','-e',code], {timeout:180000,maxBuffer:1024*1024});
    const samples = output.stdout.trim().split(/\r?\n/).map(JSON.parse);
    await writeFile(resolve(dir,'result.json'),JSON.stringify({id,commandCode:code,samples},null,2));
    console.log(JSON.stringify({directory:dir,samples}));
  } finally {
    await exec('docker',['rm','-f',container],{timeout:30000}).catch(()=>{});
    const inspect=JSON.parse((await exec('docker',['volume','inspect',volume],{timeout:30000})).stdout);
    if(inspect[0].Labels['pulsechat.ioProbe']===id) await exec('docker',['volume','rm',volume],{timeout:30000});
  }
})().catch(e=>{console.error(String(e));process.exitCode=1});
