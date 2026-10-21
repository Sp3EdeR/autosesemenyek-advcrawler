const { createHash } = require('node:crypto');
const { readFile, writeFile, appendFile } = require('node:fs/promises');

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

/** Optional invalid end dates fall back to the start, as they do in project fields. */
function expired(start, end, cutoff) {
  const endTime = dateOnly(end) ? Date.parse(end) : NaN;
  const time = Number.isNaN(endTime) ? Date.parse(start) : endTime;
  return time <= cutoff;
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

/** Queue mutations across creation and update phases without replaying completed batches. */
class MutationBuilder {
  static #batchSize = 20;

  constructor(api, project, fields) {
    this.api = api;
    this.project = project;
    this.fields = fields;
    this.updates = [];
  }

  setField(item, name, value) {
    const input = { projectId: this.project, itemId: item, fieldId: this.fields[name].id };
    if (value === null) {
      this.#prepare(
        'ClearProjectV2ItemFieldValueInput!',
        'clearProjectV2ItemFieldValue',
        'projectV2Item { id }',
        input
      );
    } else {
      input.value = value;
      this.#prepare(
        'UpdateProjectV2ItemFieldValueInput!',
        'updateProjectV2ItemFieldValue',
        'projectV2Item { id }',
        input
      );
    }
  }

  create(event) {
    this.#prepare(
      'AddProjectV2DraftIssueInput!',
      'addProjectV2DraftIssue',
      'projectItem { id }',
      { projectId: this.project, title: event.summary, body: event.body }
    );
  }

  refresh(draftIssueId, event) {
    this.#prepare(
      'UpdateProjectV2DraftIssueInput!',
      'updateProjectV2DraftIssue',
      'draftIssue { id }',
      { draftIssueId, title: event.summary, body: event.body }
    );
  }

  delete(itemId) {
    this.#prepare(
      'DeleteProjectV2ItemInput!',
      'deleteProjectV2Item',
      'deletedItemId',
      { projectId: this.project, itemId }
    );
  }

  #prepare(type, field, selection, input) {
    const index = this.updates.length;
    this.updates.push({
      alias: `m${index}`,
      variable: `input${index}`,
      declaration: `$input${index}: ${type}`,
      selection: `m${index}: ${field}(input: $input${index}) { ${selection} }`,
      input,
    });
  }

  async run() {
    const results = [];
    const batchSize = MutationBuilder.#batchSize;
    console.log(`Running ${this.updates.length} mutation(s), batch size ${batchSize}.`);
    while (this.updates.length) {
      const batch = this.updates.slice(0, batchSize);
      const variables = Object.fromEntries(batch.map(update => [update.variable, update.input]));
      const declarations = batch.map(update => update.declaration).join(', ');
      console.log(`Applying ${batch.length} mutation(s).`);
      const data = await this.api(`mutation Batch(${declarations}) {
        ${batch.map(update => update.selection).join('\n')}
      }`, variables);
      const batchResults = batch.map(update => data[update.alias]);
      results.push(...batchResults);
      console.log(`${batchResults.filter(Boolean).length} mutations finished successfully.`);
      this.updates.splice(0, batch.length);
    }
    return results;
  }
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
  console.log(`Found project ${owner}/${number}.`);
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
  console.log(`Verified the project's fields.`);
  // Fetch the existing project items.
  const items = await pages(
    api,
    `query Items($project: ID!, $cursor: String) {
      node(id: $project) { ... on ProjectV2 {
        items(first: 100, after: $cursor) {
          nodes {
            id isArchived
            content { __typename ... on DraftIssue { id title body } }
            eventId: fieldValueByName(name: "ID") { ... on ProjectV2ItemFieldTextValue { text } }
            start: fieldValueByName(name: "Start") { ... on ProjectV2ItemFieldDateValue { date } }
            end: fieldValueByName(name: "End") { ... on ProjectV2ItemFieldDateValue { date } }
            location: fieldValueByName(name: "Location") {
              ... on ProjectV2ItemFieldTextValue { text }
            }
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
  console.log(`Fetched ${items.length} project item(s).`);
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

  const mutations = new MutationBuilder(api, project, fields);

  // Unique events
  const seen = new Set();
  for (const event of events) {
    if (seen.has(event.id))
      throw new Error(`Duplicate input event ID: ${event.id}`);
    seen.add(event.id);
  }
  const cutoff = Date.now() - 14 * 24 * 60 * 60 * 1000;
  events = events.filter(event => !expired(event.dtstart, event.dtend, cutoff));

  // Delete expired project items.
  let deleted = 0;
  for (const [id, item] of existing) {
    if (expired(item.start?.date, item.end?.date, cutoff)) {
      mutations.delete(item.id);
      existing.delete(id);
      deleted++;
    }
  }

  // Create new project items for fresh events.
  const freshEvents = events.filter(event => !existing.has(event.id));
  for (const event of freshEvents)
    mutations.create(event);
  // Deletions are queued first, so their results precede the creation results.
  console.log(`Deleting ${deleted} expired & creating ${freshEvents.length} new event(s).`);
  const created = (await mutations.run()).slice(deleted); // < MUTATION HERE
  const newItems = new Map(freshEvents.map((event, index) =>
    [event.id, created[index].projectItem]));

  // Process each event, creating or updating project items as necessary.
  const report = { events: [] };
  for (const event of events) {
    // Check if the event already has a corresponding project item.
    let item = existing.get(event.id);
    const fresh = !item;
    const unfinished = item && !item.eventId?.text && !item.status?.name;
    if (fresh) {
      item = newItems.get(event.id);
    } else if (item.content.title !== event.summary || item.content.body !== event.body) {
      mutations.refresh(item.content.id, event);
    }
    // Body markers recover drafts if creation succeeds but a later batch fails.
    if (fresh || unfinished || item.status?.name === 'Old')
      mutations.setField(item.id, 'Status', { singleSelectOptionId: fields.statuses.Todo });
    if (item.eventId?.text !== event.id)
      mutations.setField(item.id, 'ID', { text: event.id });
    const start = dateOnly(event.dtstart);
    if ((item.start?.date || null) !== start)
      mutations.setField(item.id, 'Start', { date: start });
    const end = dateOnly(event.dtend);
    if ((item.end?.date || null) !== end)
      mutations.setField(item.id, 'End', end ? { date: end } : null);
    const location = event.location || null;
    if ((item.location?.text || null) !== location)
      mutations.setField(item.id, 'Location', location ? { text: location } : null);

    // Create a report file
    const oldStatus = item?.status?.name ?? null;
    const newStatus = fresh || unfinished || oldStatus === 'Old' ? 'Todo' : oldStatus;
    report.events.push({
      event_id: event.id,
      item_id: item.id,
      summary: event.summary,
      start: event.dtstart,
      end: dateOnly(event.dtend),
      location: event.location || null,
      is_new: fresh,
      old_status: oldStatus,
      new_status: newStatus
    });
  }

  // Mark absent Todo items as Old.
  for (const [id, item] of existing) {
    if (!seen.has(id) && item.status?.name === 'Todo')
      mutations.setField(item.id, 'Status', { singleSelectOptionId: fields.statuses.Old });
  }

  console.log(`Applying event data field mutations.`);
  await mutations.run(); // < MUTATION HERE

  console.log(`Synced ${events.length} events, deleted ${deleted} expired items.`);
  return report;
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
  const report = await sync(client, input('project_owner'), number, events);
  const reportFile = input('report_file');
  await writeFile(reportFile, JSON.stringify(report, null, 2), 'utf8');
  await appendFile(process.env.GITHUB_OUTPUT, `report_file=${reportFile}\n`);
}

module.exports = { normalize, dateOnly, eventId, markdown, description, loadEvents,
  createClient, validateFields, MutationBuilder, sync };
if (require.main === module) main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});