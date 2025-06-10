# Cloudflare KV Manager

A TypeScript CLI tool for managing Cloudflare KV namespaces. Built to handle large-scale cache management with intelligent key grouping.

## What it does

This tool helps you manage Cloudflare KV entries at scale. It's particularly useful when you have thousands of cache entries with related keys (like video files with multiple derivatives and chunks). Instead of manually managing individual keys, you can work with logical file identifiers.

## Setup

### Prerequisites
- Node.js v18+
- A Cloudflare account with KV namespace access
- API token with KV read/write permissions

### Installation

```bash
# Clone or download the project
cd kv-manager
npm install
```

### Configuration

Create `config.json` in the project root:

```json
{
  "accountId": "YOUR_CLOUDFLARE_ACCOUNT_ID",
  "namespaceId": "YOUR_KV_NAMESPACE_ID", 
  "apiToken": "YOUR_CLOUDFLARE_API_TOKEN"
}
```

⚠️ Add `config.json` to your `.gitignore` - it contains sensitive credentials.

### Getting your credentials

1. **Account ID**: Found in Cloudflare dashboard → Right sidebar
2. **Namespace ID**: Workers & Pages → KV → View your namespace
3. **API Token**: My Profile → API Tokens → Create Token with "Account:Workers KV Storage:Edit" permission

## Usage

### Running commands

```bash
# Development (TypeScript)
npm run dev -- <command>

# Production (compile first)
npm run build
npm start -- <command>
```

### Commands

#### List keys
Fetches all keys from your KV namespace and groups them by file identifier.

```bash
# List all keys
npm run dev -- list

# List keys with specific prefix
npm run dev -- list -p "video:"

# Save to custom file
npm run dev -- list -o my-keys.json
```

Output format:
```json
{
  "14aa49e6500959b4": [
    "direct_/m/14aa49e6500959b4/original/file.mp4_chunk_1",
    "direct_/m/14aa49e6500959b4/original/file.mp4_chunk_2",
    "video:m/14aa49e6500959b4/original/file.mp4:derivative=desktop"
  ],
  "1459487_fe003865": [
    "video:videos/1459487_fe003865.mp4:derivative=desktop",
    "video:videos/1459487_fe003865.mp4:derivative=mobile",
    "video:videos/1459487_fe003865.mp4:derivative=tablet"
  ]
}
```

#### Delete keys
Removes all cache entries for a specific file identifier.

```bash
# Delete using hex ID
npm run dev -- delete "14aa49e6500959b4"

# Delete using core ID
npm run dev -- delete "1459487_fe003865"

# Use custom key file
npm run dev -- rm "1459487_fe003865" -i my-keys.json

# Skip staleness check
npm run dev -- delete "14aa49e6500959b4" --force
```

The tool will:
1. Find all keys associated with the identifier
2. Show you what will be deleted
3. Delete all related entries (chunks, derivatives, etc.)
4. Update the local key file

#### Write keys
Add new entries to KV.

```bash
# Simple key-value
npm run dev -- put "my-key" "my value"

# With TTL (seconds)
npm run dev -- put "temp-key" "temporary data" --ttl 3600

# With metadata
npm run dev -- put "user:123" "user data" --metadata '{"type":"user","version":2}'
```

## Key format handling

The tool intelligently groups cache keys by extracting common identifiers. It supports multiple key formats:

### 1. Hex ID format (16-character identifiers)
Keys containing 16-character hexadecimal IDs are grouped by that ID:
```
video:m/14aa49e6500959b4/original/file.mp4:derivative=desktop
direct_/m/14aa49e6500959b4/original/file.mp4_chunk_1
→ Identifier: 14aa49e6500959b4
```

### 2. Core ID extraction from paths
For paths containing files with pattern `{number}_{alphanumeric}`, extracts just the core ID:
```
video:videos/1459487_fe003865.mp4:derivative=tablet
direct_/videos/0743860_fe001008_v1.mp4_chunk_1
→ Identifier: 1459487_fe003865 and 0743860_fe001008_v1
```

### 3. Colon-separated cache format
Format: `{content-type}:{path}:{params}` - extracts the path component:
```
video:m/path/to/file.mp4:derivative=mobile_chunk_1
video:custom/path/document.pdf:derivative=thumbnail
→ Identifier: path/to/file.mp4 or custom/path/document.pdf
```

### 4. Direct chunk format
Keys with `direct_` prefix and chunk numbers:
```
direct_/m/123/original/video.mp4_chunk_1
direct_/m/123/original/video.mp4_chunk_2
→ Identifier: 123/original/video.mp4
```

### 5. URL path format
Keys containing `/m/` in the path:
```
https://example.com/m/videos/123/file.mp4
→ Identifier: videos/123/file.mp4
```

### Chunk sorting

The tool automatically sorts chunks numerically:
- `_chunk_1, _chunk_2, ..., _chunk_10, _chunk_11` (correct order)
- Not `_chunk_1, _chunk_10, _chunk_11, _chunk_2` (alphabetical)

This applies to both suffix chunks (`file_chunk_N`) and parameter chunks (`:derivative=desktop_chunk_N`).

## Performance tips

- The tool caches key listings locally to avoid repeated API calls
- It warns you if the cache is over 24 hours old
- Use prefixes (`-p`) when working with specific content types
- The tool handles pagination automatically for large namespaces

## Common workflows

### Purge all cache entries for a video
```bash
npm run dev -- list
npm run dev -- delete "1459487_fe003865"  # Just use the core ID
```

### List and manage specific content types
```bash
npm run dev -- list -p "image:" -o images.json
npm run dev -- delete "images/header.jpg" -i images.json
```

### Batch operations
```bash
# List all videos
npm run dev -- list -p "video:" -o videos.json

# Then use the JSON file with other tools or scripts
cat videos.json | jq 'keys[]' | while read id; do
  echo "Processing $id"
  # Your custom logic here
done
```

## Troubleshooting

### "Local key file not found"
Run `list` command first to create the key file.

### "Identifier not found"
The tool will automatically refresh from Cloudflare. If still not found, check the exact identifier in your key file.

### TypeScript errors
Ensure you're using Node.js v18+ and have run `npm install`.

## Development

```bash
# Watch mode for development
npm run build:watch

# Type checking
npx tsc --noEmit

# Run directly with tsx (fastest for development)
npm run dev -- <command>
```

The codebase uses:
- Cloudflare's official SDK
- Yargs for CLI parsing
- Inquirer for interactive prompts
- TypeScript with strict mode