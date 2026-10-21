const { readFile, writeFile, appendFile } = require('node:fs/promises');
const { join } = require('node:path');

function escapeHtml(value) {
  const entities = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
  return String(value).replace(/[&<>"']/g, character => entities[character]);
}

// Sort by local wall-clock time; date-only values sort before timed ones.
function sortKey(start) {
  return start.replace(' ', 'T');
}

function render(report, template, owner, number) {
  const events = report.events.filter(event => event.is_new === true)
    .sort((a, b) => sortKey(a.start).localeCompare(sortKey(b.start)));
  if (!events.length)
    return null;
  const url = `https://github.com/users/${encodeURIComponent(owner)}/projects/${encodeURIComponent(number)}`;
  const values = {
    project_url: escapeHtml(url),
    new_count: events.length,
    event_titles: events.map(event => {
      const label = escapeHtml(`${event.start.slice(0, 10)} - ${event.summary}`);
      return `<li style="margin:0 0 14px;overflow-wrap:anywhere;">${label}</li>`;
    }).join('\n')
  };
  return template.replace(
    /\{\{(project_url|new_count|event_titles)\}\}/g,
    (_, name) => values[name]
  );
}

async function main() {
  const report = JSON.parse(await readFile(process.env.INPUT_REPORT_FILE, 'utf8'));
  const template = await readFile(join(__dirname, 'template.html'), 'utf8');
  const html = render(
    report,
    template,
    process.env.INPUT_PROJECT_OWNER,
    process.env.INPUT_PROJECT_NUMBER
  );
  const htmlFile = html === null ? '' : process.env.INPUT_HTML_FILE;
  if (html !== null)
    await writeFile(htmlFile, html, 'utf8');
  await appendFile(process.env.GITHUB_OUTPUT, `html_file=${htmlFile}\n`);
}

module.exports = { render };
if (require.main === module)
  main().catch(error => {
    console.error(error);
    process.exitCode = 1;
  });