# Cloudflare KV Manager Script

---

## Prerequisites

* [Node.js](https://nodejs.org/) (version 18 or later)
* npm (usually included with Node.js)

---

## Installation & Setup

1.  **Place Files**:
    Place the script (`kv-manager.ts` or `index.ts`), `package.json`, and `tsconfig.json` in a new directory.

2.  **Install Dependencies**:
    Navigate to your project directory in the terminal and run:
    ```bash
    npm install
    ```

3.  **Create Configuration File**:
    Create a file named `config.json` in the same directory. Copy the structure below into the file and add your Cloudflare credentials.

    **`config.json`:**
    ```json
    {
      "accountId": "YOUR_CLOUDFLARE_ACCOUNT_ID",
      "namespaceId": "YOUR_KV_NAMESPACE_ID",
      "apiToken": "YOUR_CLOUDFLARE_API_TOKEN"
    }
    ```
    * **Security Note**: This file contains secrets. Be sure to add `config.json` to your `.gitignore` file to prevent it from being committed to version control.

---

## Usage

The script is run from the command line using `npx ts-node <your-script-name>.ts`.

### Viewing Help

A detailed help menu is available for all commands.

```bash
# Get general help and a list of all commands
npx ts-node kv-manager.ts --help

# Get help for a specific command (e.g., 'list')
npx ts-node kv-manager.ts list --help
```

### `list` (or `ls`)
Lists keys from the KV namespace and saves them to a JSON file.

* **List all keys and save to the default `kv-keys.json`:**
    ```bash
    npx ts-node kv-manager.ts list
    ```

* **List keys starting with a prefix and save to a custom file:**
    ```bash
    npx ts-node kv-manager.ts list --prefix "video:" --output "videos.json"
    ```

### `delete` (or `rm`)
Deletes all keys associated with a given identifier found in the JSON file.

* **Delete keys using the default `kv-keys.json`:**
    ```bash
    npx ts-node kv-manager.ts delete "videos/1473359_fe004134.mp4"
    ```

* **Delete keys using a custom JSON file:**
    ```bash
    npx ts-node kv-manager.ts rm "videos/1473359_fe004134.mp4" -i "videos.json"
    ```
    *(Using the `rm` alias and `-i` shorthand)*

* **Force deletion without checking if the file is stale:**
    ```bash
    npx ts-node kv-manager.ts delete "some_identifier" --force
    ```

### `put`
Writes a new key-value pair to the KV namespace.

* **Write a simple key and value:**
    ```bash
    npx ts-node kv-manager.ts put "my-key" "This is the value"
    ```

* **Write a key that expires in one hour (3600 seconds):**
    ```bash
    npx ts-node kv-manager.ts put "temp-key" "This is temporary" --ttl 3600
    ```

* **Write a key with metadata:**
    The `--metadata` flag requires a valid JSON string. On most terminals, wrap the JSON in single quotes (`'`) to avoid issues with the double quotes inside.
    ```bash
    npx ts-node kv-manager.ts put "user:101" '{"name":"Alice"}' --metadata '{"source":"script","version":2}'
    ```

---

## Identifier Parsing Logic

The script intelligently groups related keys by extracting a common identifier. It tries the following patterns in order:

1.  **Colon-Separated Format**: For keys like `type:path:details` (e.g., `video:videos/123.mp4:derivative=tablet`), it extracts the middle `path` part (`videos/123.mp4`) as the identifier.
2.  **URL Path Format**: For keys resembling a URL path like `.../m/identifier/...`, it extracts the `identifier`.
3.  **Fallback**: If no pattern matches, the entire key is used as its own unique identifier.
