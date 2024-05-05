import { Command } from "commander";
import { z } from "zod";
import { Database } from "bun:sqlite";
import * as OperatingSystem from "os";
import * as Path from "path";
import * as FileSystem from "fs/promises";
import { DateTime } from "luxon";
import { DefaultAzureCredential, type TokenCredential } from "@azure/identity";
import { ResourceManagementClient } from "@azure/arm-resources";
import { 
    StorageManagementClient, 
    type StorageAccount 
} from "@azure/arm-storage";

import { 
    ShareServiceClient,
} from "@azure/storage-file-share";

//#region General
type Success<T> = { readonly success: true, readonly data: T };
type Failure<E> = { readonly success: false, readonly error: E };
type Result<T, E> = Success<T> | Failure<E>;

const resourceTypeSchema = z.enum([ "Microsoft.Storage/storageAccounts" ]);
type ResourceType = z.infer<typeof resourceTypeSchema>;

const unixDateTimeSchema = z.number().transform(
    (unixTimestamp) => DateTime.fromSeconds(unixTimestamp));

const fileShareNameSchema = z.string()
    .regex(/^([a-z]|[0-9])([a-z]|[0-9]|(-(?!-))){1,61}([a-z]|[0-9])$/);

const fileShareSchema = z.object({
    fileShareId: z.string().uuid(),
    dataPlaneUri: z.string().url(),
    resourceName: z.string().min(3),
    resourceType: resourceTypeSchema,
    resourceCreateDate: unixDateTimeSchema,
    fileShareName: fileShareNameSchema,
    isSnapshot: z.coerce.boolean(),
    snapshotTime: unixDateTimeSchema.nullable()
});

type FileShare = z.infer<typeof fileShareSchema>;
//#endregion

//#region SQLite logic
type CacheDbPathFailure = {
    readonly failureType: "CacheDbPathFailure";
    readonly dbParentDirectory: string;
    readonly failureDetail: unknown;
};

type CacheDbConnectionFailure = {
    readonly failureType: "CacheDbConnectionFailure";
    readonly dbPath: string;
    readonly failureDetail: unknown;
};

type CacheDbQueryFailure = {
    readonly failureType: "CacheDbQueryFailure";
    readonly dbPath: string;
    readonly query: string;
    readonly failureDetail: unknown;
};

type CacheDbResultSchemaFailure = {
    readonly failureType: "CacheDbResultSchemaFailure";
    readonly dbPath: string;
    readonly failureDetail: unknown;
};

type CacheDbInitFailure = {
    readonly failureType: "CacheDbInitFailure";
    readonly failureDetail: CacheDbPathFailure | CacheDbConnectionFailure | 
        CacheDbQueryFailure | CacheDbResultSchemaFailure;
};

interface ICacheDb extends AsyncDisposable {
    init(): Promise<Result<void, CacheDbInitFailure>>;
}

const sqliteDataTypeSchema = z.enum([ "INTEGER", "REAL", "TEXT", "BLOB" ]);
type SqliteDataType = z.infer<typeof sqliteDataTypeSchema>;

type SqliteColumn = {
    name: string;
    type: SqliteDataType;
    nullable: boolean;
    primaryKey: boolean;
}

const sqliteSchemaSchema = z.array(z.object({
    cid: z.number().gte(0),
    name: z.string(),
    type: sqliteDataTypeSchema.nullable(),
    notnull: z.coerce.boolean(),
    dflt_value: z.unknown(),
    pk: z.number()
}));

type SqliteSchema = z.infer<typeof sqliteSchemaSchema>;

class DefaultCacheDb implements ICacheDb {
    protected connection: Database | null = null;

    protected readonly fileSharesColumns = Object.freeze(
        new Map<string, SqliteColumn>([
            [
                "fileShareId",
                { 
                    name: "fileShareId", 
                    type: "TEXT", 
                    nullable: false, 
                    primaryKey: true 
                },
            ],
            [
                "dataPlaneUri",
                { 
                    name: "dataPlaneUri", 
                    type: "TEXT", 
                    nullable: false,
                    primaryKey: true
                },
            ],
            [
                "resourceName",
                { 
                    name: "resourceName", 
                    type: "TEXT", 
                    nullable: false,
                    primaryKey: false
                }
            ],
            [
                "resourceType",
                { 
                    name: "resourceType", 
                    type: "TEXT", 
                    nullable: false,
                    primaryKey: false,
                },
            ],
            [
                "resourceCreateDate",
                { 
                    name: "resourceCreateDate", 
                    type: "INTEGER", 
                    nullable: false,
                    primaryKey: false
                },
            ],
            [
                "fileShareName",
                { 
                    name: "fileShareName", 
                    type: "TEXT", 
                    nullable: false,
                    primaryKey: false
                },
            ],
            [
                "isSnapshot",
                { 
                    name: "isSnapshot", 
                    type: "INTEGER", 
                    nullable: false,
                    primaryKey: false,
                },
            ],
            [
                "snapshotTime",
                { 
                    name: "snapshotTime", 
                    type: "INTEGER",
                    nullable: false,
                    primaryKey: false
                }
            ]            
        ]));

    protected readonly fileSystemEntriesColumns = Object.freeze(
        new Map<string, SqliteColumn>([
            [
                "fileShareId",
                { 
                    name: "fileShareId", 
                    type: "TEXT", 
                    nullable: false, 
                    primaryKey: true 
                },
            ],
            [
                "dataPlaneUri",
                {
                    name: "dataPlaneUri",
                    type: "TEXT",
                    nullable: false,
                    primaryKey: true
                },
            ],
            [
                "path",
                {
                    name: "path",
                    type: "TEXT",
                    nullable: false, 
                    primaryKey: true
                },
            ],
            [
                "entryName",
                { 
                    name: "entryName", 
                    type: "TEXT", 
                    nullable: false,
                    primaryKey: false
                }
            ]
        ]));

    protected readonly dbPath: string;

    public constructor(dbPath?: string) {
        if (dbPath === undefined) {
            let basePath: string;
            if (OperatingSystem.platform() === "win32" && 
                process.env.LOCALAPPDATA !== undefined
            ) {
                basePath = process.env.LOCALAPPDATA;
            } 
            else {
                basePath = OperatingSystem.homedir();
            }
            
            dbPath = Path.join(basePath, ".recursiveSearch", "cache.db");
        }

        this.dbPath = dbPath;
    }

    public async init(): Promise<Result<void, CacheDbInitFailure>> {
        // Create containing directory if it doesn't exist
        const parentDirectory = Path.dirname(this.dbPath);
        try {
            FileSystem.mkdir(parentDirectory, { recursive: true });
        }
        catch (error) {
            return {
                success: false,
                error: {
                    failureType: "CacheDbInitFailure",
                    failureDetail: {
                        failureType: "CacheDbPathFailure",
                        dbParentDirectory: parentDirectory,
                        failureDetail: error
                    }
                }
            };
        }

        // Create connection to SQLite database
        if (this.connection !== null) {
            throw new Error(`CacheDb has already been initialized ` + 
                `(db path: ${this.dbPath}).`);
        }

        try {
            this.connection = new Database(this.dbPath, { create: true });
        }
        catch (error) {
            return {
                success: false,
                error: {
                    failureType: "CacheDbInitFailure",
                    failureDetail: {
                        failureType: "CacheDbConnectionFailure",
                        dbPath: this.dbPath,
                        failureDetail: error
                    }
                }
            };
        }

        // Set up connection
        this.connection.run("PRAGMA journal_mode=WAL");

        // Validate the FileShares table if it exists
        const fsSchemaQuery = "PRAGMA table_info(FileShares)";
        let fsSchemaQueryResult: ReadonlyArray<unknown>;
        try {
            using fsSchemaStmt = this.connection.prepare(fsSchemaQuery);
            fsSchemaQueryResult = fsSchemaStmt.all();
        }
        catch (error) {
            return {
                success: false,
                error: {
                    failureType: "CacheDbInitFailure",
                    failureDetail: {
                        failureType: "CacheDbQueryFailure",
                        dbPath: this.dbPath,
                        query: fsSchemaQuery,
                        failureDetail: error
                    }
                }
            };
        }

        const parsedFsSchemaResult = 
            sqliteSchemaSchema.safeParse(fsSchemaQueryResult);
        if (!parsedFsSchemaResult.success) {
            return {
                success: false,
                error: {
                    failureType: "CacheDbInitFailure",
                    failureDetail: {
                        failureType: "CacheDbResultSchemaFailure",
                        dbPath: this.dbPath,
                        failureDetail: parsedFsSchemaResult.error
                    }
                }
            };
        }

        let createFsTable = false;
        if (parsedFsSchemaResult.data.length === 0) {
            createFsTable = true;
        }
        else {
            createFsTable = !DefaultCacheDb.columnsMatch(
                this.fileSharesColumns, 
                parsedFsSchemaResult.data
            );
        }

        if (createFsTable) {
            const dropFsTableQuery = "DROP TABLE IF EXISTS FileShares";
            try {
                this.connection.run(dropFsTableQuery);
            }
            catch (error) {
                return {
                    success: false,
                    error: {
                        failureType: "CacheDbInitFailure",
                        failureDetail: {
                            failureType: "CacheDbQueryFailure",
                            dbPath: this.dbPath,
                            query: dropFsTableQuery,
                            failureDetail: error
                        }
                    }
                };
            }

            const dropEntriesTableQuery = 
                "DROP TABLE IF EXISTS FileSystemEntries";
            try {
                this.connection.run(dropEntriesTableQuery);
            }
            catch (error) {
                return {
                    success: false,
                    error: {
                        failureType: "CacheDbInitFailure",
                        failureDetail: {
                            failureType: "CacheDbQueryFailure",
                            dbPath: this.dbPath,
                            query: dropFsTableQuery,
                            failureDetail: error
                        }
                    }
                };
            }

            const createFsTableQuery = DefaultCacheDb.buildCreateTableQuery(
                "FileShares", 
                this.fileSharesColumns
            );
            try {
                this.connection.run(createFsTableQuery);
            }
            catch (error) {
                return {
                    success: false,
                    error: {
                        failureType: "CacheDbInitFailure",
                        failureDetail: {
                            failureType: "CacheDbQueryFailure",
                            dbPath: this.dbPath,
                            query: createFsTableQuery,
                            failureDetail: error
                        }
                    }
                };
            }
        }

        // Validate the FileSystemEntries table if it exists
        const entriesSchemaQuery = "PRAGMA table_info(FileSystemEntries)";
        let entriesSchemaQueryResult: ReadonlyArray<unknown>;
        try {
            using entriesSchemaStmt = 
                this.connection.prepare(entriesSchemaQuery);
            entriesSchemaQueryResult = entriesSchemaStmt.all();
        }
        catch (error) {
            return {
                success: false,
                error: {
                    failureType: "CacheDbInitFailure",
                    failureDetail: {
                        failureType: "CacheDbQueryFailure",
                        dbPath: this.dbPath,
                        query: entriesSchemaQuery,
                        failureDetail: error
                    }
                }
            };
        }

        const parsedEntriesSchemaResult = 
            sqliteSchemaSchema.safeParse(entriesSchemaQueryResult);
        if (!parsedEntriesSchemaResult.success) {
            return {
                success: false,
                error: {
                    failureType: "CacheDbInitFailure",
                    failureDetail: {
                        failureType: "CacheDbResultSchemaFailure",
                        dbPath: this.dbPath,
                        failureDetail: parsedEntriesSchemaResult.error
                    }
                }
            };
        }

        let createEntriesTable = false;
        if (parsedEntriesSchemaResult.data.length === 0) {
            createEntriesTable = true;
        }
        else {
            createEntriesTable = !DefaultCacheDb.columnsMatch(
                this.fileSystemEntriesColumns, 
                parsedEntriesSchemaResult.data
            );
        }

        if (createEntriesTable) {
            const dropEntriesTableQuery = 
                "DROP TABLE IF EXISTS FileSystemEntries";
            try {
                this.connection.run(dropEntriesTableQuery);
            }
            catch (error) {
                return {
                    success: false,
                    error: {
                        failureType: "CacheDbInitFailure",
                        failureDetail: {
                            failureType: "CacheDbQueryFailure",
                            dbPath: this.dbPath,
                            query: dropEntriesTableQuery,
                            failureDetail: error
                        }
                    }
                };
            }

            const createEntriesTblQuery = DefaultCacheDb.buildCreateTableQuery(
                "FileSystemEntries",
                this.fileSystemEntriesColumns
            );
            try {
                this.connection.run(createEntriesTblQuery);
            }
            catch (error) {
                return {
                    success: false,
                    error: {
                        failureType: "CacheDbInitFailure",
                        failureDetail: {
                            failureType: "CacheDbQueryFailure",
                            dbPath: this.dbPath,
                            query: createEntriesTblQuery,
                            failureDetail: error
                        }
                    }
                };
            }
        }

        return { success: true, data: undefined };
    }

    protected static columnsMatch(
        expectedColumns: ReadonlyMap<string, SqliteColumn>,
        sqliteSchema: SqliteSchema
    ): boolean {
        const foundColumns = new Map<string, SqliteColumn>();
        for (const rawColumn of sqliteSchema) {
            if (rawColumn.type === null) {
                return false;
            }

            const foundColumn: SqliteColumn = {
                name: rawColumn.name,
                type: rawColumn.type,
                nullable: !rawColumn.notnull,
                primaryKey: Boolean(rawColumn.pk)
            };

            const expectedColumn = expectedColumns.get(foundColumn.name);
            if (expectedColumn === undefined && !foundColumn.nullable) {
                return false;
            }
            else if (expectedColumn === undefined) {
                continue;
            }

            if (expectedColumn.type !== foundColumn.type ||
                expectedColumn.nullable !== foundColumn.nullable ||
                expectedColumn.primaryKey !== expectedColumn.primaryKey
            ) {
                return false;
            }

            foundColumns.set(foundColumn.name, foundColumn);
        }

        for (const expectedColumn of expectedColumns.values()) {
            const foundColumn = foundColumns.get(expectedColumn.name);
            if (foundColumn === undefined) {
                return false;
            }

            if (expectedColumn.type !== foundColumn.type ||
                expectedColumn.nullable !== foundColumn.nullable ||
                expectedColumn.primaryKey !== expectedColumn.primaryKey
            ) {
                return false;
            }
        }

        return true;
    }

    protected static buildCreateTableQuery(
        tableName: string, 
        columns: ReadonlyMap<string, SqliteColumn>
    ): string {
        let createTableQuery = `CREATE TABLE "${tableName}" (`;
        const primaryKeys = new Array<string>();
        let i = 0;
        for (const column of columns.values()) {
            if (i > 0) {
                createTableQuery += ", ";
            }

            if (column.primaryKey) {
                primaryKeys.push(column.name);
            }

            createTableQuery += `"${column.name}" ${column.type}`;
            if (!column.nullable) {
                createTableQuery += " NOT NULL";
            }

            i++;
        }

        if (primaryKeys.length > 0) {
            createTableQuery += ", PRIMARY KEY ("
        }

        for (let j = 0; j < primaryKeys.length; j++) {
            if (j > 0) {
                createTableQuery += ", ";
            }

            createTableQuery += `"${primaryKeys[j]}"`;
        }

        if (primaryKeys.length > 0) {
            createTableQuery += ")";
        }

        createTableQuery += ") STRICT";
        return createTableQuery;
    }

    public async [Symbol.asyncDispose](): Promise<void> {
        this.connection?.close(false);
    }

}
//#endregion

//#region Azure logic
type UnknownSubscriptionFailure = {
    readonly failureType: "UnknownSubscriptionFailure";
    readonly subscriptionId: string;
};

type GetFileShareDetailsFailure = {
    readonly failureType: "GetFileShareDetailsFailure";
    readonly failureDetail: unknown;
};

interface IFileShareAdapter {
    getFileShareDetails(): Promise<Result<
        ReadonlyArray<FileShare>, 
        GetFileShareDetailsFailure
    >>;
}

class DefaultFileShareAdapter implements IFileShareAdapter {
    protected resourceManagementClient: ResourceManagementClient | null = null;

    public constructor(
        protected readonly subscriptionId: string,
        protected readonly resourceGroupName: string,
        protected readonly resourceName: string,
        protected readonly fileShareName: string,
        protected readonly credential: TokenCredential = 
            new DefaultAzureCredential()
    ) {
    }

    public async getFileShareDetails(): Promise<Result<
        ReadonlyArray<FileShare>, 
        GetFileShareDetailsFailure
    >> {
        this.resourceManagementClient = 
            new ResourceManagementClient(this.credential, this.subscriptionId);
        

        // Information on storage account and file share for test
        // const subscriptionId = "1d16f9b3-bbe3-48d4-930a-27a74dca003b";
        // const resourceGroupName = "wgries-mfsres";
        // const storageAccountName = "wgriesmfsressa3";

        // const azureCred = new DefaultAzureCredential();

        // const storageMgmtClient = 
        //     new StorageManagementClient(azureCred, subscriptionId);

        // let storageAccount: StorageAccount;
        // try {
        //     storageAccount = await storageMgmtClient.storageAccounts.getProperties(
        //         resourceGroupName, storageAccountName);
        // }
        // catch (error) {
        //     console.error(`Could not get storage account info: ${error}`);
        //     process.exit(1);
        // }

        // console.log(storageAccount.sku?.name);
        throw new Error("Method not implemented.");
    }

}
//#endregion

//#region CLI logic
const program = new Command();

program
    .name("recursiveSearch")
    .description(
        "A quick tool to recursively search a file share using the " +
        "FileREST API."
    )
    .version("0.0.1");

const searchScopeSchema = z.enum([ "Both", "FileShare", "FileShareSnapshots" ]);
type SearchScope = z.infer<typeof searchScopeSchema>;

const matchBehaviorSchema = z.enum([ "End", "ScopeEnd", "Continue" ]);
type MatchBehavior = z.infer<typeof matchBehaviorSchema>;

const searchArgsSchema = z.object({
    subscription: z.string().uuid(),
    resourceGroup: z.string().min(1).max(90),
    storageAccount: z.string().regex(/^[a-z0-9]{3,24}$/),
    fileShare: z.string()
        .regex(/^([a-z]|[0-9])([a-z]|[0-9]|(-(?!-))){1,61}([a-z]|[0-9])$/),
    targetItem: z.string(),
    searchScope: searchScopeSchema,
    matchBehavior: matchBehaviorSchema
});

program.command("search")
    .requiredOption(
        "--subscription <subscription-id>", 
        "The subscription ID containing the target file share."
    )
    .requiredOption(
        "--resource-group <resource-group-name>",
        "The resource group containing the target file share."
    )
    .requiredOption(
        "--storage-account <storage-account-name>",
        "The storage account containing the target file share."
    )
    .requiredOption(
        "--file-share <file-share-name>",
        "The name of the file share to search."
    )
    .requiredOption(
        "--target-item <target-item-name>",
        "The item (file or directory) to search for."
    )
    .option(
        "--search-scope <scope>",
        "The scope over which to search: FileShare, FileShareSnapshots, " +
            "or Both (default).",
        "Both"
    )
    .option(
        "--match-behavior <match-behavior>",
        "Specify the behavior when a match is found: End, ScopeEnd " + 
            "(default), or Continue.",
        "ScopeEnd"
    )
    .action(async function (args: unknown) {
        const searchArgsResult = searchArgsSchema.safeParse(args);
        if (!searchArgsResult.success) {
            console.error(JSON.stringify(searchArgsResult.error));
            process.exit(1);
        }

        const searchArgs = searchArgsResult.data;

        await using cacheDb: ICacheDb = new DefaultCacheDb();
        const initResult = await cacheDb.init();
        if (!initResult.success) {
            console.error(JSON.stringify(initResult.error));
            process.exit(1);
        }
    });

program.parse();
//#endregion