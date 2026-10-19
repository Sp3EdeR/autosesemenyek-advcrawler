const { createHash } = require('node:crypto');
const { readFile } = require('node:fs/promises');

/** Normalize identity text without depending on punctuation, accents or spacing. */
function normalize(value) {
  return value.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase()
    .replace(/[^\p{L}\p{N}]/gu, '');
}

/** Extract the event's local calendar date, never shifting it through UTC. */
function dateOnly(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}(?:$|T| )/.test(value))
    return null;
  const day = value.slice(0, 10);
  const parsed = new Date(`${day}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime()))
    return null;
  return parsed.toISOString().slice(0, 10) !== day ? null : day;
}

/** Hash only identity fields; adding sources or changing event times keeps the ID. */
function eventId(event) {
  const identity = [normalize(event.summary), dateOnly(event.dtstart),
    normalize(event.location || '')];
  return `event-v1-${createHash('sha256').update(JSON.stringify(identity)).digest('hex')}`;
}

/** Decode common HTML entities while leaving unknown names intact. */
function decodeEntities(text) {
  const entities = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ' };
  return text.replace(/&(#x[\da-f]+|#\d+|amp|lt|gt|quot|apos|nbsp);/gi, (entity, name) => {
    if (!name.startsWith('#')) 
      return entities[name.toLowerCase()];
    const hex = name[1].toLowerCase() === 'x';
    const point = Number.parseInt(name.slice(hex ? 2 : 1), hex ? 16 : 10);
    return point > 0 && point <= 0x10ffff &&
      !(point >= 0xd800 && point <= 0xdfff) ? String.fromCodePoint(point) : entity;
  });
}

/** Keep only HTTP(S) links; angle brackets protect Markdown URLs containing parentheses. */
function linkTarget(value) {
  try {
    const url = new URL(decodeEntities(value).trim());
    return ['http:', 'https:'].includes(url.protocol)
      ? url.href.replace(/</g, '%3C').replace(/>/g, '%3E') : null;
  } catch {
    return null;
  }
}

/** Convert common crawler HTML, preserving existing Markdown and Unicode text. */
function markdown(value = '') {
  return decodeEntities(value
    .replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, '')
    .replace(/<a\b[^>]*href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))[^>]*>([\s\S]*?)<\/a>/gi,
      (_, double, single, bare, label) => {
        const target = linkTarget(double ?? single ?? bare);
        const text = label.replace(/<[^>]+>/g, '').replace(/([\[\]])/g, '\\$1');
        return target ? `[${text}](<${target}>)` : text;
      })
    .replace(/<(strong|b)\b[^>]*>([\s\S]*?)<\/\1\s*>/gi, '**$2**')
    .replace(/<(em|i)\b[^>]*>([\s\S]*?)<\/\1\s*>/gi, '*$2*')
    .replace(/<h([1-6])\b[^>]*>([\s\S]*?)<\/h\1\s*>/gi,
      (_, level, text) => `\n\n${'#'.repeat(Number(level))} ${text}\n\n`)
    .replace(/<li\b[^>]*>/gi, '\n- ').replace(/<\/li\s*>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/?(?:p|div|ul|ol|blockquote)\b[^>]*>/gi, '\n\n')
    .replace(/<\/?[a-z][a-z0-9]*(?:\s[^>]*|\s*\/?)>/gi, '')
  ).replace(/\r\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
}

/** Put source links first, then descriptive text and metadata, including original times. */
function description(event, id) {
  const links = `${event.url || ''}\n${event.urls || ''}`;
  const urls = [...new Set(links.match(/https?:\/\/[^\s<>"']+/g) || [])];
  const sections = urls.map(url => linkTarget(url)).filter(Boolean).map(url => `- <${url}>`);
  const body = markdown(event.description);
  if (body)
    sections.push(`\n## Description\n\n${body}`);
  const metadata = [['Start', event.dtstart], ['End', event.dtend],
    ['Location', event.location], ['Geo coordinate', event.geo], ['Src Id', event.src_id]];
  sections.push('\n## Details\n');
  for (const [label, value] of metadata)
    if (value)
      sections.push(`**${label}:** ${markdown(value)}\n`);
  sections.push(`<!-- sync-project:${id} -->`);
  return sections.join('\n').trim();
}

/** Validate the complete snapshot before performing any project mutations. */
function loadEvents(raw) {
  if (!Array.isArray(raw))
    throw new Error('Events JSON must contain an array.');
  return raw.flatMap(group => Object.entries(group).map(([src_id, payload]) => {
    const event = { ...payload, src_id };
    if (typeof event.summary !== 'string' || !event.summary.trim() || !dateOnly(event.dtstart))
      throw new Error(`Event ${src_id} requires a title and valid start date.`);
    const id = eventId(event);
    return { ...event, id, body: description(event, id) };
  }));
}

/** GraphQL transport fails loudly on HTTP errors and partial GraphQL responses. */
function createClient(token, endpoint = 'https://api.github.com/graphql') {
  return async (query, variables) => {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json',
        'User-Agent': 'event-dashboard-sync' },
      body: JSON.stringify({ query, variables }),
      signal: AbortSignal.timeout(60000),
    });
    if (!response.ok)
      throw new Error(`GitHub GraphQL HTTP ${response.status}`);
    const result = await response.json();
    if (result.errors?.length)
      throw new Error(JSON.stringify(result.errors));
    return result.data;
  };
}

/** Page through connections so project size never changes sync semantics. */
async function pages(api, query, variables, connection) {
  const nodes = [];
  let cursor = null;
  do {
    const data = await api(query, { ...variables, cursor });
    const page = connection(data);
    nodes.push(...page.nodes);
    cursor = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
  } while (cursor);
  return nodes;
}

/** Validate the user-configured schema, including all three required status options. */
function validateFields(fields) {
  const result = {};
  for (const [name, type] of Object.entries({ Start: 'DATE', End: 'DATE', Location: 'TEXT',
    ID: 'TEXT', Status: 'SINGLE_SELECT' })) {
    const field = fields.find(value => value.name === name);
    if (field?.dataType !== type)
      throw new Error(`Project needs ${name} with type ${type}.`);
    result[name] = field;
  }
  result.statuses = {};
  for (const name of ['Todo', 'Old', 'Done']) {
    const option = result.Status.options.find(value => value.name === name);
    if (!option)
      throw new Error(`Status needs an option named ${name}.`);
    result.statuses[name] = option.id;
  }
  return result;
}

/** Sync managed drafts only; never mutate Assignees or unrelated project items. */
async function sync(api, owner, number, events) {
  // Fetch the project ID for the given owner and project number.
  const data = await api(
    `query Project($owner: String!, $number: Int!) {
      repositoryOwner(login: $owner) {
        ... on User { projectV2(number: $number) { id } }
        ... on Organization { projectV2(number: $number) { id } }
      }
    }`,
    { owner, number }
  );
  const project = data.repositoryOwner?.projectV2?.id;
  if (!project)
    throw new Error(`Project ${owner}/${number} not found or not accessible.`);
  // Fetch and validate the project fields.
  const fields = validateFields(await pages(
    api,
    `query Fields($project: ID!, $cursor: String) {
      node(id: $project) { ... on ProjectV2 {
        fields(first: 100, after: $cursor) {
          nodes {
            ... on ProjectV2Field { id name dataType }
            ... on ProjectV2SingleSelectField { id name dataType options { id name } }
          }
          pageInfo { hasNextPage endCursor }
        }
      } }
    }`,
    { project },
    value => value.node.fields
  ));
  // Fetch the existing project items.
  // TODO: Add server-side filtering for relevant IDs to reduce data transfer.
  const items = await pages(
    api,
    `query Items($project: ID!, $cursor: String) {
      node(id: $project) { ... on ProjectV2 {
        items(first: 100, after: $cursor) {
          nodes {
            id isArchived
            content { __typename ... on DraftIssue { id body } }
            eventId: fieldValueByName(name: "ID") { ... on ProjectV2ItemFieldTextValue { text } }
            status: fieldValueByName(name: "Status") {
              ... on ProjectV2ItemFieldSingleSelectValue { name }
            }
          }
          pageInfo { hasNextPage endCursor }
        }
      } }
    }`,
    { project },
    value => value.node.items
  );
  // Map existing project items by their managed ID for quick lookup.
  const existing = new Map();
  for (const item of items) {
    if (item.isArchived || item.content?.__typename !== 'DraftIssue')
      continue;
    const id = item.eventId?.text || item.content.body?.match(
      /<!-- sync-project:(event-v1-[a-f0-9]{64}) -->/)?.[1];
    if (!/^event-v1-[a-f0-9]{64}$/.test(id || ''))
      continue;
    if (existing.has(id))
      throw new Error(`Duplicate managed project ID: ${id}`);
    existing.set(id, item);
  }

  // Helper function to update a field value for an existing project item.
  async function setField(item, name, value) {
    if (value === null)
      await api(
        `mutation Clear($input: ClearProjectV2ItemFieldValueInput!) {
          clearProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
        }`,
        { input: { projectId: project, itemId: item, fieldId: fields[name].id } }
      );
    else
      await api(
        `mutation Set($input: UpdateProjectV2ItemFieldValueInput!) {
          updateProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
        }`,
        { input: { projectId: project, itemId: item, fieldId: fields[name].id, value } }
      );
  }

  // Unique events
  const seen = new Set();
  for (const event of events) {
    if (seen.has(event.id))
      throw new Error(`Duplicate input event ID: ${event.id}`);
    seen.add(event.id);
  }
  // Process each event, creating or updating project items as necessary.
  for (const event of events) {
    // Check if the event already has a corresponding project item.
    let item = existing.get(event.id);
    const fresh = !item;
    const unfinished = item && !item.eventId?.text && !item.status?.name;
    if (fresh) {
      const created = await api(
        `mutation Create($input: AddProjectV2DraftIssueInput!) {
          addProjectV2DraftIssue(input: $input) { projectItem { id } }
        }`,
        { input: { projectId: project, title: event.summary, body: event.body } }
      );
      item = created.addProjectV2DraftIssue.projectItem;
    } else {
      await api(
        `mutation Refresh($input: UpdateProjectV2DraftIssueInput!) {
          updateProjectV2DraftIssue(input: $input) { draftIssue { id } }
        }`,
        { input: { draftIssueId: item.content.id, title: event.summary, body: event.body } }
      );
    }
    // Write identity first; the body marker recovers drafts if this first write fails.
    if (fresh || unfinished || item.status?.name === 'Old')
      await setField(item.id, 'Status', { singleSelectOptionId: fields.statuses.Todo });
    await setField(item.id, 'ID', { text: event.id });
    await setField(item.id, 'Start', { date: dateOnly(event.dtstart) });
    const end = dateOnly(event.dtend);
    await setField(item.id, 'End', end ? { date: end } : null);
    await setField(item.id, 'Location', event.location ? { text: event.location } : null);
  }

  // Mark absent Todo items as Old.
  let old = 0;
  for (const [id, item] of existing) {
    if (!seen.has(id) && item.status?.name === 'Todo') {
      await setField(item.id, 'Status', { singleSelectOptionId: fields.statuses.Old });
      old++;
    }
  }
  console.log(`Synced ${events.length} events; marked ${old} absent Todo items Old.`);
}

/** Read action inputs without needing an Actions SDK or installed dependencies. */
async function main() {
  function input(name) {
    const value = process.env[`INPUT_${name.toUpperCase()}`]?.trim();
    if (!value)
      throw new Error(`Missing action input: ${name}`);
    return value;
  }
  const client = createClient(input('project_token'), process.env.GITHUB_GRAPHQL_URL);
  const number = Number(input('project_number'));
  if (!Number.isSafeInteger(number) || number < 1)
    throw new Error('Invalid project number.');
  const events = loadEvents(JSON.parse(await readFile(input('events_file'), 'utf8')));
  await sync(client, input('project_owner'), number, events);
}

module.exports = { normalize, dateOnly, eventId, markdown, description, loadEvents,
  createClient, validateFields, sync };
if (require.main === module) main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});