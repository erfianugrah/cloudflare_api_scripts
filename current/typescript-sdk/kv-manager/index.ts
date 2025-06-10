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

  private extractIdentifier(key: string): string {
    let match = key.match(/^video:videos\/([a-zA-Z0-9_]+)\.mp4/);
    if (match && match[1]) {
      return match[1];
    }

    const pathParts = key.split("/");
    const mIndex = pathParts.indexOf("m");
    if (mIndex > -1 && pathParts.length > mIndex + 1) {
      return pathParts[mIndex + 1];
    }

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
    // Improved command definition for 'list'
    .command(
      "list",
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
    // Improved command definition for 'delete'
    .command(
      "delete <identifier>",
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
              "$0 delete 1473234_fe004132",
              "Delete all keys for the given ID using kv-keys.json.",
            ],
            [
              "$0 delete some-other-id -i my-keys.json",
              "Delete using a custom input file.",
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
    .demandCommand(1, "You must provide a valid command (list or delete).")
    .strict() // Throws an error for unknown commands or options
    .alias("h", "help") // Adds '-h' as an alias for '--help'
    .epilogue("For more information, please check the documentation.")
    .argv;
}

main();
