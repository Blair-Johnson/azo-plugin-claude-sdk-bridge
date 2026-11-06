import { inspect } from 'node:util';

// Preserve SDK errors, including stacks, causes, and provider response details.
export function errorText(error) {
  return typeof error === 'string' ? error : inspect(error, {
    depth: null, colors: false, customInspect: false,
    maxArrayLength: null, maxStringLength: null,
  });
}
