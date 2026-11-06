# Reference adapter

This implementation adapts the provider-boundary design of pi-claude-bridge by Eli Dickinson, version 0.10.0, commit `7923e38e1de81dd9562e75754061dd631488e7f3`: https://github.com/elidickinson/pi-claude-bridge . In particular, it follows the ID-keyed parked MCP tool-result bridge, full assistant batch boundaries, persistent SDK query, isolated child configuration, and host-owned history imported through cc-session-io. The HTTP Messages transport and Agent Zoo configuration are specific to this adaptation. Dependency licenses are also distributed in the installed packages.

## pi-claude-bridge license

MIT License

Copyright (c) 2026 Eli Dickinson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
