import Cloudflare from "cloudflare";
import * as fs from "fs/promises";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import inquirer from "inquirer";

const CONFIG_FILE = "config.json";
const CACHE_STALE_HOURS = 24;

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
   * Extracts a common identifier from various key formats.
   * @param key The key string from KV.
   * @returns A common identifier or the full key as a fallback.
   */
  private extractIdentifier(key: string): string {
    const colonParts = key.split(":");

    // Handles formats like "type:path:details" (e.g., "video:videos/123.mp4:derivative=tablet")
    // by extracting the middle 'path' part. This is the most generic pattern.
    if (colonParts.length > 1) {
      // We return the second part, which is assumed to be the main identifier.
      // e.g., "videos/1473234_fe004132.mp4"
      return colonParts[1].trim();
    }

    // Fallback for the original URL format (e.g., "https://.../m/identifier/...")
    const pathParts = key.split("/");
    const mIndex = pathParts.indexOf("m");
    if (mIndex > -1 && pathParts.length > mIndex + 1) {
      return pathParts[mIndex + 1];
    }

    // If no other pattern matches, return the full key.
    return key;
  }

  async listAndStoreKeys(
    prefix?: string,
    outputFilename: string = "kv-keys.json",
  ) {
    if (prefix) {
      console.log(
        `Fetching all keys from namespace with prefix: "${prefix}"...`,
      );
    } else {
      console.log("Fetching all keys from namespace...");
    }

    try {
      const allKeys: { [identifier: string]: string[] } = {};
      let keyCount = 0;

      const listParams: Cloudflare.KV.Namespaces.Keys.KeyListParams = {
        account_id: this.accountId,
      };

      if (prefix) {
        listParams.prefix = prefix;
      }

      const keys = this.client.kv.namespaces.keys.list(
        this.namespaceId,
        listParams,
      );

      for await (const key of keys) {
        const identifier = this.extractIdentifier(key.name);

        if (!allKeys[identifier]) {
          allKeys[identifier] = [];
        }

        allKeys[identifier].push(key.name);
        keyCount++;
      }

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

  async deleteKeyByIdentifier(
    identifierToDelete: string,
    force: boolean,
    inputFilename: string = "kv-keys.json",
  ) {
    console.log(
      `Attempting to delete key(s) with identifier: ${identifierToDelete}`,
    );
    try {
      if (!force) {
        try {
          const stats = await fs.stat(inputFilename);
          const hoursSinceModified = (Date.now() - stats.mtimeMs) /
            (1000 * 60 * 60);
          if (hoursSinceModified > CACHE_STALE_HOURS) {
            const { shouldRefresh } = await inquirer.prompt([
              {
                type: "confirm",
                name: "shouldRefresh",
                message:
                  `The key list in '${inputFilename}' is over ${CACHE_STALE_HOURS} hours old. It's recommended to refresh it first. Refresh now?`,
                default: true,
              },
            ]);
            if (shouldRefresh) {
              await this.listAndStoreKeys(undefined, inputFilename);
            }
          }
        } catch (error: any) {
          if (error.code !== "ENOENT") throw error;
        }
      }

      let keys;
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

      let keysToDelete = keys[identifierToDelete];

      if (!keysToDelete || keysToDelete.length === 0) {
        console.log(
          "Identifier not found in local cache. Refreshing from Cloudflare and trying again...",
        );
        const refreshedKeys = await this.listAndStoreKeys(
          undefined,
          inputFilename,
        );
        if (refreshedKeys) {
          keysToDelete = refreshedKeys[identifierToDelete];
        }
      }

      if (!keysToDelete || keysToDelete.length === 0) {
        console.log(
          `Identifier not found even after refreshing: ${identifierToDelete}`,
        );
        return;
      }

      console.log(
        `Found ${keysToDelete.length} key(s) to delete for identifier '${identifierToDelete}':`,
      );

      const deletePromises = keysToDelete.map((keyName: string) => {
        console.log(`  - Deleting ${keyName}`);
        return this.client.kv.namespaces.values.delete(
          this.namespaceId,
          keyName,
          {
            account_id: this.accountId,
          },
        );
      });

      await Promise.all(deletePromises);

      const currentKeys = JSON.parse(await fs.readFile(inputFilename, "utf-8"));
      delete currentKeys[identifierToDelete];
      await fs.writeFile(inputFilename, JSON.stringify(currentKeys, null, 2));

      console.log(
        `Successfully deleted all keys for identifier: ${identifierToDelete}`,
      );
    } catch (error: any) {
      console.error("An unexpected error occurred during deletion:", error);
    }
  }

  async putKey(
    key: string,
    value: string,
    ttl?: number,
    metadataJSON?: string,
  ) {
    console.log(`Writing to key: "${key}"...`);
    try {
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

      if (ttl) {
        body.expiration_ttl = ttl;
        console.log(`  With TTL: ${ttl} seconds`);
      }

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

async function main() {
  let config;
  try {
    const configData = await fs.readFile(CONFIG_FILE, "utf-8");
    config = JSON.parse(configData);
  } catch (error) {
    console.error(
      `Error reading config file: ${CONFIG_FILE}. Please create it based on config.json.example.`,
    );
    return;
  }

  const kvManager = new KvManager(
    config.apiToken,
    config.accountId,
    config.namespaceId,
  );

  yargs(hideBin(process.argv))
    .usage("Usage: $0 <command> [options]")
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
            description:
              "Force delete without checking if the local key list is stale.",
          })
          .option("input", {
            alias: "i",
            type: "string",
            default: "kv-keys.json",
            description: "The JSON file to read keys from.",
          })
          .example([
            [
              "$0 delete videos/1473359_fe004134.mp4",
              "Delete all keys for the given ID using kv-keys.json.",
            ],
            [
              "$0 rm some-other-id -i my-keys.json",
              "Delete using a custom input file and the 'rm' alias.",
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
    .demandCommand(
      1,
      "You must provide a valid command (list, delete, or put).",
    )
    .strict()
    .alias("h", "help")
    .wrap(yargs.terminalWidth).argv;
}

main();
