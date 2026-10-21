const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { mkdtemp, writeFile, readFile, rm } = require('node:fs/promises');
const { tmpdir } = require('node:os');
const { execFileSync } = require('node:child_process');
const { render } = require('./index');

const template = readFileSync(join(__dirname, 'template.html'), 'utf8');
const report = { events: [
    { is_new: true, summary: 'Aut\u00f3s tal\u00e1lkoz\u00f3 <script> & "friends"',
      start: '2026-05-02 10:00' },
    { is_new: true, summary: "Rally '26 {{new_count}}", start: '2026-05-01' },
    { is_new: false, summary: 'Returning Old event', start: '2026-04-01' },
    { is_new: false, summary: 'Absent event', start: '2026-04-02' },
  ] };

test('renders Hungarian email with user dashboard link and only new event titles', () => {
  const html = render(report, template, 'owner', '7');
  assert.match(html, /lang="hu"/);
  assert.match(html, /2 &uacute;j esem&eacute;ny/);
  assert.ok(html.includes('href="https://github.com/users/owner/projects/7"'));
  assert.ok(html.includes('Aut\u00f3s tal\u00e1lkoz\u00f3 &lt;script&gt; &amp; &quot;friends&quot;'));
  assert.ok(html.includes('Rally &#39;26 {{new_count}}'));
  assert.equal((html.match(/<li /g) || []).length, 2);
  assert.ok(!html.includes('Returning Old event'));
  assert.ok(!html.includes('Absent event'));
  assert.ok(!html.includes('<script>'));
  assert.ok(html.indexOf('2026-05-01 - Rally') < html.indexOf('2026-05-02 - Aut'));
});

test('encodes project inputs instead of allowing URL path or attribute injection', () => {
  const html = render(report, template, 'owner/"', '7?x=1&y=2');
  assert.ok(html.includes('href="https://github.com/users/owner%2F%22/projects/7%3Fx%3D1%26y%3D2"'));
});

test('does not render notifications for empty syncs or existing items', () => {
  assert.equal(render({ events: [] }, template, 'owner', '7'), null);
  const events = report.events.filter(event => !event.is_new);
  assert.equal(render({ events }, template, 'owner', '7'), null);
});

for (const hasNewEvents of [true, false]) {
test(`action entry point ${hasNewEvents ? 'writes HTML' : 'skips HTML without new events'}`,
  async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dashboard-email-'));
  try {
    const reportFile = join(directory, 'report.json');
    const htmlFile = join(directory, 'email.html');
    const outputFile = join(directory, 'outputs');
    const input = hasNewEvents ? report : { events: [{ is_new: false, summary: 'Existing', start: '2026-05-01' }] };
    await writeFile(reportFile, JSON.stringify(input), 'utf8');
    execFileSync(process.execPath, [join(__dirname, 'index.js')], { env: { ...process.env,
      INPUT_REPORT_FILE: reportFile, INPUT_HTML_FILE: htmlFile, INPUT_PROJECT_OWNER: 'owner',
      INPUT_PROJECT_NUMBER: '7', GITHUB_OUTPUT: outputFile } });
    if (hasNewEvents)
      assert.equal(await readFile(htmlFile, 'utf8'), render(input, template, 'owner', '7'));
    else
      await assert.rejects(readFile(htmlFile, 'utf8'), { code: 'ENOENT' });
    assert.equal(await readFile(outputFile, 'utf8'),
      `html_file=${hasNewEvents ? htmlFile : ''}\n`);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
}