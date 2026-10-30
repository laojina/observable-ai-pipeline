const { chromium } = require(require.resolve('playwright', { paths: process.env.PIPELINE_NODE_MODULES ? [process.env.PIPELINE_NODE_MODULES] : [__dirname] }));
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const net = require('net');
const root = path.resolve(__dirname, '..');
const directory = path.join(root, 'target', 'ui-' + Date.now());
fs.mkdirSync(directory, { recursive: true });

async function main() {
    await new Promise((resolve, reject) => {
        const probe = net.createServer();
        probe.once('error', reject);
        probe.listen(18084, '127.0.0.1', () => probe.close(resolve));
    });
    const log = fs.openSync(path.join(directory, 'server.log'), 'a');
    const java = process.env.PIPELINE_JAVA || (process.env.JAVA_HOME ? path.join(process.env.JAVA_HOME, 'bin', process.platform === 'win32' ? 'java.exe' : 'java') : 'java');
    const server = spawn(java, ['-jar', path.join(root, 'target/recoverable-pipeline-0.1.0.jar'), '--server.port=18084', '--pipeline.data=' + directory], { cwd: root, stdio: ['ignore', log, log], windowsHide: true });
    fs.closeSync(log);
    let browser;
    try {
        for (let attempt = 0; attempt < 120; attempt++) {
            if (server.exitCode !== null) throw Error('Test server exited during startup');
            try { if ((await fetch('http://127.0.0.1:18084/api/tasks')).ok) break; } catch {}
            await new Promise(resolve => setTimeout(resolve, 300));
        }
        browser = await chromium.launch({ ...(process.env.PIPELINE_BROWSER ? { executablePath: process.env.PIPELINE_BROWSER } : process.platform === 'win32' ? { channel: 'msedge' } : {}), headless: true });
        const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
        const errors = [];
        page.on('pageerror', error => errors.push(error.message));
        await page.goto('http://127.0.0.1:18084');
        await page.locator('#asset').fill('ui-demo-' + Date.now());
        await page.locator('#duration').fill('50');
        await page.locator('#submit').click();
        await page.waitForFunction(() => document.getElementById('selection').textContent.includes('SUCCEEDED'), { timeout: 30000 });
        if (!(await page.locator('#selection').textContent()).includes('4/4')) throw Error('Missing completion count');
        if (!(await page.locator('#steps tr').nth(2).locator('td').nth(2).textContent()).includes('2')) throw Error('Retry count not displayed');
        await page.getByRole('button', { name: '查看产物', exact: true }).first().click();
        await page.waitForFunction(() => document.getElementById('artifact').textContent.includes('fingerprint'));
        if (!JSON.parse(await page.locator('#artifact').textContent()).mock) throw Error('Artifact is not explicitly marked Mock');
        if (errors.length) throw Error(JSON.stringify(errors));
        await page.screenshot({ path: path.join(directory, 'dashboard.png'), fullPage: false });
        fs.writeFileSync(path.join(directory, 'ui-report.json'), JSON.stringify({ passed: ['submit from browser', 'poll until complete', 'show stage retry count', 'read verified artifact', 'no JavaScript errors'], errors }, null, 2));
        console.log('PASS: browser submit, polling, retries, artifact view, no JavaScript errors');
        console.log('Evidence: ' + directory);
    } finally {
        if (browser) await browser.close();
        if (server.exitCode === null) {
            server.kill();
            await new Promise(resolve => server.once('exit', resolve));
        }
    }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
