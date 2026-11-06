const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);

export function invalid(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode, publicMessage: message });
}

export function validateRequest(value) {
  if (!object(value)) throw invalid('Request must be a JSON object.');
  const accepted = new Set(['model', 'messages', 'system', 'tools', 'stream', 'max_tokens',
    'thinking', 'output_config', 'tool_choice', 'metadata', 'cache_control']);
  for (const key of Object.keys(value)) {
    if (!accepted.has(key)) throw invalid(`Unsupported Messages parameter: ${key.slice(0, 64)}.`);
  }
  if (typeof value.model !== 'string' || value.model.length > 128 || value.model !== value.model.trim()
      || !/^[A-Za-z0-9._:-]+(?:\[1m\])?$/.test(value.model))
    throw invalid('A model name is required.');
  if (!Array.isArray(value.messages) || !value.messages.length)
    throw invalid('A nonempty messages array is required.');
  if (!Number.isSafeInteger(value.max_tokens) || value.max_tokens < 1 || value.max_tokens > 128000)
    throw invalid('max_tokens must be an integer from 1 to 128000.');
  if (value.stream !== undefined && typeof value.stream !== 'boolean')
    throw invalid('stream must be boolean.');
  if (value.system !== undefined && typeof value.system !== 'string' &&
      !(Array.isArray(value.system) && value.system.every(b => object(b) && b.type === 'text' && typeof b.text === 'string')))
    throw invalid('system must be text or an array of text blocks.');
  if (value.tools != null && !Array.isArray(value.tools)) throw invalid('tools must be an array.');
  for (const tool of value.tools ?? []) validateSchemaCompatibility(tool?.input_schema);
  if (value.tool_choice !== undefined && (!object(value.tool_choice) || value.tool_choice.type !== 'auto' ||
      Object.keys(value.tool_choice).some(k => k !== 'type')))
    throw invalid('Only automatic tool selection is supported by this bridge.');
  if (value.output_config !== undefined && (!object(value.output_config) ||
      Object.keys(value.output_config).some(k => k !== 'effort') ||
      !['low', 'medium', 'high', 'max'].includes(value.output_config.effort)))
    throw invalid('output_config supports only effort: low, medium, high, or max.');
  if (value.thinking !== undefined) {
    const t = value.thinking;
    if (!object(t) || !['adaptive', 'enabled', 'disabled'].includes(t.type) ||
        Object.keys(t).some(k => !['type', 'budget_tokens'].includes(k)) ||
        (t.type === 'enabled' && (!Number.isSafeInteger(t.budget_tokens) || t.budget_tokens < 1024)) ||
        (t.type !== 'enabled' && t.budget_tokens !== undefined))
      throw invalid('Unsupported thinking configuration.');
  }
  for (const message of value.messages) {
    if (!object(message) || !['user', 'assistant'].includes(message.role) ||
        !(typeof message.content === 'string' || Array.isArray(message.content)))
      throw invalid('Messages must contain user/assistant roles and text or content blocks.');
  }
  if (value.messages.at(-1).role !== 'user') throw invalid('The final message must be a user turn.');
  return { ...value, tools: value.tools ?? [] };
}

export function validateSessionId(value) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value))
    throw invalid('Set x-azo-session-id to the Agent Zoo session ID.');
  return value;
}

export function validateSchemaCompatibility(schema) {
  if (object(schema) && ['anyOf', 'oneOf', 'allOf'].some(key => Object.hasOwn(schema, key)))
    throw invalid('Top-level anyOf/oneOf/allOf tool schemas are not supported by the Claude SDK.');
}
