#!/usr/bin/env node
import Cloudflare from "cloudflare";
import * as fs from "fs/promises";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import inquirer from "inquirer";

// ===== Configuration =====
const CONFIG_FILE = "config.json";
const CACHE_STALE_HOURS = 24;

// ===== Type Definitions =====
interface Config {
  apiToken: string;
  accountId: string;
  namespaceId: string;
}

interface KeyGroup {
  [identifier: string]: string[];
}

interface ChunkInfo {
  chunkNumber: number | null;
  baseKey: string;
}

// ===== KV Manager Class =====
class KvManager {
  private client: Cloudflare;
  private accountId: string;
  private namespaceId: string;

  constructor(apiToken: string, accountId: string, namespaceId: string) {
    this.client = new Cloudflare({ apiToken });
    this.accountId = accountId;
    this.namespaceId = namespaceId;
  }

  /**
   * Extracts a common identifier from various KV key formats.
   * 
   * Supported formats:
   * 1. Hex ID format: Keys containing 16-character hex IDs (e.g., "14aa49e6500959b4")
   *    - video:m/14aa49e6500959b4/original/file.mp4:derivative=desktop
   *    - direct_/m/14aa49e6500959b4/original/file.mp4_chunk_1
   * 
   * 2. Colon-separated format: {content-type}:{path}:{params}
   *    - video:videos/1476492_fe004163.mp4:derivative=tablet
   *    - video:m/path/to/file.mp4:derivative=mobile_chunk_1
   * 
   * 3. Direct chunk format: direct_{path}_chunk_{number}
   *    - direct_/m/123/original/video.mp4_chunk_1
   * 
   * 4. URL path format: Contains /m/ in the path
   *    - https://example.com/m/identifier/file.mp4
   */
  private extractIdentifier(key: string): string {
    // Priority 1: Extract 16-character hex ID if present
    const hexIdMatch = key.match(/\b([0-9a-f]{16})\b/);
    if (hexIdMatch) {
      return hexIdMatch[1];
    }

    // Helper function to extract core ID from a path
    const extractCoreId = (path: string): string => {
      // Look for ID patterns anywhere in the path
      // Pattern 1: 16-char hex (already handled above)
      // Pattern 2: number_alphanumeric (e.g., 1459487_fe003865)
      const idMatch = path.match(/\b(\d+_[a-f0-9]+)\b/i);
      if (idMatch) {
        return idMatch[1];
      }
      
      // Pattern 3: If path ends with a filename, extract it without extension
      const parts = path.split('/');
      if (parts.length > 1) {
        const filename = parts[parts.length - 1];
        const nameWithoutExt = filename.replace(/\.[^/.]+$/, '');
        
        // If the filename looks like it could be an ID, return it
        // This handles cases where the ID doesn't match our expected patterns
        if (nameWithoutExt && !nameWithoutExt.includes('/')) {
          return nameWithoutExt;
        }
      }
      
      // Fallback: return the full path
      return path;
    };

    // Priority 2: Handle colon-separated cache format
    const colonParts = key.split(":");
    if (colonParts.length >= 2) {
      const pathPart = colonParts[1].trim();
      
      // Remove m/ prefix if present
      if (pathPart.startsWith('m/')) {
        const cleanPath = pathPart.substring(2);
        return extractCoreId(cleanPath);
      }
      
      return extractCoreId(pathPart);
    }

    // Priority 3: Handle direct_ prefix format
    if (key.startsWith('direct_')) {
      let cleanKey = key.substring(7); // Remove 'direct_'
      
      // Remove chunk suffix if present
      const chunkMatch = cleanKey.match(/^(.+?)_chunk_\d+$/);
      if (chunkMatch) {
        cleanKey = chunkMatch[1];
      }
      
      // Remove /m/ prefix if present
      if (cleanKey.startsWith('/m/')) {
        cleanKey = cleanKey.substring(3);
      }
      
      return extractCoreId(cleanKey);
    }

    // Priority 4: Handle URL-like paths with /m/
    const pathParts = key.split("/");
    const mIndex = pathParts.indexOf("m");
    if (mIndex > -1 && pathParts.length > mIndex + 1) {
      const path = pathParts.slice(mIndex + 1).join('/');
      return extractCoreId(path);
    }

    // Fallback: Return the original key
    return key;
  }

  /**
   * Extracts chunk information from a key.
   * Handles both _chunk_N suffix and :param_chunk_N formats.
   */
  private extractChunkInfo(key: string): ChunkInfo {
    let chunkNumber: number | null = null;
    let baseKey = key;
    
    // Check for _chunk_N at the end
    const suffixMatch = key.match(/_chunk_(\d+)$/);
    if (suffixMatch) {
      chunkNumber = parseInt(suffixMatch[1]);
      baseKey = key.substring(0, key.lastIndexOf('_chunk_'));
      return { chunkNumber, baseKey };
    }
    
    // Check for chunk in params (e.g., :derivative=desktop_chunk_0)
    if (key.includes(':')) {
      const paramMatch = key.match(/[_:]chunk_(\d+)/);
      if (paramMatch) {
        chunkNumber = parseInt(paramMatch[1]);
        baseKey = key.replace(/_chunk_\d+/, '');
      }
    }
    
    return { chunkNumber, baseKey };
  }

  /**
   * Custom sort function for KV keys.
   * Sorts chunks numerically and other keys alphabetically.
   */
  private sortKeys(a: string, b: string): number {
    const aInfo = this.extractChunkInfo(a);
    const bInfo = this.extractChunkInfo(b);
    
    // If both have chunk numbers and same base, sort numerically
    if (aInfo.chunkNumber !== null && 
        bInfo.chunkNumber !== null && 
        aInfo.baseKey === bInfo.baseKey) {
      return aInfo.chunkNumber - bInfo.chunkNumber;
    }
    
    // Otherwise, sort alphabetically
    return a.localeCompare(b);
  }

  /**
   * Lists all keys from the KV namespace and groups them by identifier.
   */
  async listAndStoreKeys(
    prefix?: string,
    outputFilename: string = "kv-keys.json",
  ): Promise<KeyGroup | null> {
    const prefixMsg = prefix 
      ? `Fetching all keys from namespace with prefix: "${prefix}"...`
      : "Fetching all keys from namespace...";
    console.log(prefixMsg);

    try {
      const allKeys: KeyGroup = {};
      let keyCount = 0;

      // Build list parameters
      const listParams: Cloudflare.KV.Namespaces.Keys.KeyListParams = {
        account_id: this.accountId,
      };

      if (prefix) {
        listParams.prefix = prefix;
      }

      // Fetch keys from Cloudflare KV
      const keys = this.client.kv.namespaces.keys.list(
        this.namespaceId,
        listParams,
      );

      // Group keys by identifier
      for await (const key of keys) {
        const identifier = this.extractIdentifier(key.name);

        if (!allKeys[identifier]) {
          allKeys[identifier] = [];
        }

        allKeys[identifier].push(key.name);
        keyCount++;
      }

      // Sort keys within each group
      for (const identifier in allKeys) {
        allKeys[identifier].sort((a, b) => this.sortKeys(a, b));
      }

      // Save to file
      await fs.writeFile(outputFilename, JSON.stringify(allKeys, null, 2));
      console.log(
        `Successfully processed ${keyCount} keys into ${outputFilename}`,
      );
      
      return allKeys;
    } catch (error) {
      console.error("Error listing and storing keys:", error);
      return null;
    }
  }

  /**
   * Checks if the cache file is stale and prompts for refresh if needed.
   */
  private async checkCacheStaleness(
    inputFilename: string, 
    force: boolean
  ): Promise<void> {
    if (force) return;

    try {
      const stats = await fs.stat(inputFilename);
      const hoursSinceModified = (Date.now() - stats.mtimeMs) / (1000 * 60 * 60);
      
      if (hoursSinceModified > CACHE_STALE_HOURS) {
        const { shouldRefresh } = await inquirer.prompt([
          {
            type: "confirm",
            name: "shouldRefresh",
            message: `The key list in '${inputFilename}' is over ${CACHE_STALE_HOURS} hours old. It's recommended to refresh it first. Refresh now?`,
            default: true,
          },
        ]);
        
        if (shouldRefresh) {
          await this.listAndStoreKeys(undefined, inputFilename);
        }
      }
    } catch (error: any) {
      // File doesn't exist, will be handled later
      if (error.code !== "ENOENT") throw error;
    }
  }

  /**
   * Deletes all keys associated with a given identifier.
   */
  async deleteKeyByIdentifier(
    identifierToDelete: string,
    force: boolean,
    inputFilename: string = "kv-keys.json",
  ): Promise<void> {
    console.log(
      `Attempting to delete key(s) with identifier: ${identifierToDelete}`,
    );
    
    try {
      // Check cache staleness
      await this.checkCacheStaleness(inputFilename, force);

      // Load keys from file
      let keys: KeyGroup;
      try {
        keys = JSON.parse(await fs.readFile(inputFilename, "utf-8"));
      } catch (error: any) {
        if (error.code === "ENOENT") {
          console.log(
            `Local key file '${inputFilename}' not found. Please run the 'list' command first.`,
          );
          return;
        }
        throw error;
      }

      // Find keys to delete
      let keysToDelete = keys[identifierToDelete];

      // If not found, try refreshing from Cloudflare
      if (!keysToDelete || keysToDelete.length === 0) {
        console.log(
          "Identifier not found in local cache. Refreshing from Cloudflare and trying again...",
        );
        const refreshedKeys = await this.listAndStoreKeys(undefined, inputFilename);
        if (refreshedKeys) {
          keysToDelete = refreshedKeys[identifierToDelete];
        }
      }

      // Check if identifier exists
      if (!keysToDelete || keysToDelete.length === 0) {
        console.log(
          `Identifier not found even after refreshing: ${identifierToDelete}`,
        );
        return;
      }

      // Delete all keys
      console.log(
        `Found ${keysToDelete.length} key(s) to delete for identifier '${identifierToDelete}':`,
      );

      const deletePromises = keysToDelete.map((keyName: string) => {
        console.log(`  - Deleting ${keyName}`);
        return this.client.kv.namespaces.values.delete(
          this.namespaceId,
          keyName,
          { account_id: this.accountId },
        );
      });

      await Promise.all(deletePromises);

      // Update local cache
      const currentKeys = JSON.parse(await fs.readFile(inputFilename, "utf-8"));
      delete currentKeys[identifierToDelete];
      await fs.writeFile(inputFilename, JSON.stringify(currentKeys, null, 2));

      console.log(
        `Successfully deleted all keys for identifier: ${identifierToDelete}`,
      );
    } catch (error) {
      console.error("An unexpected error occurred during deletion:", error);
    }
  }

  /**
   * Writes a key-value pair to the KV namespace.
   */
  async putKey(
    key: string,
    value: string,
    ttl?: number,
    metadataJSON?: string,
  ): Promise<void> {
    console.log(`Writing to key: "${key}"...`);
    
    try {
      // Build request body
      const body: {
        account_id: string;
        value: string;
        metadata: any;
        expiration_ttl?: number;
      } = {
        account_id: this.accountId,
        value: value,
        metadata: null,
      };

      // Add TTL if provided
      if (ttl) {
        body.expiration_ttl = ttl;
        console.log(`  With TTL: ${ttl} seconds`);
      }

      // Parse and add metadata if provided
      if (metadataJSON) {
        try {
          body.metadata = JSON.parse(metadataJSON);
          console.log(`  With metadata:`, body.metadata);
        } catch (e) {
          console.error(
            "Error: The --metadata value is not a valid JSON string.",
          );
          return;
        }
      }

      // Write to KV
      await this.client.kv.namespaces.values.update(
        this.namespaceId,
        key,
        body,
      );
      
      console.log("Successfully wrote key-value pair.");
      console.log(
        "Note: Your local JSON file may now be out of date. Run the 'list' command to refresh it.",
      );
    } catch (error) {
      console.error(`Error writing key "${key}":`, error);
    }
  }
}

// ===== Main Application =====
async function loadConfig(): Promise<Config> {
  try {
    const configData = await fs.readFile(CONFIG_FILE, "utf-8");
    return JSON.parse(configData);
  } catch (error) {
    console.error(
      `Error reading config file: ${CONFIG_FILE}. Please create it with your Cloudflare credentials.`,
    );
    process.exit(1);
  }
}

async function main() {
  const config = await loadConfig();
  const kvManager = new KvManager(
    config.apiToken,
    config.accountId,
    config.namespaceId,
  );

  // Configure CLI
  yargs(hideBin(process.argv))
    .usage("Usage: $0 <command> [options]")
    
    // List command
    .command(
      ["list", "ls"],
      "List KV keys and store them in a JSON file.",
      (yargs) => {
        return yargs
          .option("prefix", {
            alias: "p",
            type: "string",
            description: "Only list keys that start with this prefix.",
          })
          .option("output", {
            alias: "o",
            type: "string",
            default: "kv-keys.json",
            description: "The output file name for the keys JSON.",
          })
          .example([
            ["$0 list", "List all keys and save to kv-keys.json."],
            [
              '$0 list -p "video:" -o "videos.json"',
              "List only video keys and save to a custom file.",
            ],
          ]);
      },
      async (argv) => {
        await kvManager.listAndStoreKeys(argv.prefix, argv.output);
      },
    )
    
    // Delete command
    .command(
      ["delete <identifier>", "rm <identifier>"],
      "Delete one or more KV keys by a common identifier.",
      (yargs) => {
        return yargs
          .positional("identifier", {
            describe: "The common identifier for the key(s) to delete.",
            type: "string",
          })
          .option("force", {
            alias: "f",
            type: "boolean",
            description: "Force delete without checking if the local key list is stale.",
          })
          .option("input", {
            alias: "i",
            type: "string",
            default: "kv-keys.json",
            description: "The JSON file to read keys from.",
          })
          .example([
            [
              "$0 delete 14aa49e6500959b4",
              "Delete all keys for the given hex ID.",
            ],
            [
              "$0 delete videos/1473359_fe004134.mp4",
              "Delete all keys for the given file path.",
            ],
            [
              "$0 rm some-id -i my-keys.json -f",
              "Force delete using a custom input file.",
            ],
          ]);
      },
      async (argv) => {
        if (argv.identifier) {
          await kvManager.deleteKeyByIdentifier(
            argv.identifier,
            argv.force || false,
            argv.input,
          );
        }
      },
    )
    
    // Put command
    .command(
      "put <key> <value>",
      "Write a key-value pair to the KV namespace.",
      (yargs) => {
        return yargs
          .positional("key", {
            describe: "The key to write to.",
            type: "string",
          })
          .positional("value", {
            describe: "The value to store.",
            type: "string",
          })
          .option("ttl", {
            type: "number",
            description: "Time-to-live (in seconds) for the key.",
          })
          .option("metadata", {
            type: "string",
            description: "A valid JSON string to be stored as metadata.",
          })
          .example([
            ['$0 put my-key "my value"', "Write a simple key-value pair."],
            [
              '$0 put temp-key "secret" --ttl 60 --metadata \'{"owner":"test"}\'',
              "Write a key with a TTL and metadata.",
            ],
          ]);
      },
      async (argv) => {
        const key = argv.key as string;
        const value = argv.value as string;
        await kvManager.putKey(key, value, argv.ttl, argv.metadata);
      },
    )
    
    // Global options
    .demandCommand(1, "You must provide a valid command (list, delete, or put).")
    .strict()
    .alias("h", "help")
    .wrap(process.stdout.columns || 80)
    .argv;
}

// Run the application
main().catch(console.error);