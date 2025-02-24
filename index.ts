#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";

const server = new Server(
  {
    name: "mcp-server-postgres-multi-schema",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  },
);

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  console.error("Usage: npx -y mcp-server-postgres-multi-schema <database-url> [schemas]");
  console.error("Example: npx -y mcp-server-postgres-multi-schema postgresql://localhost/mydb \"public,custom_schema\"");
  process.exit(1);
}

const databaseUrl = args[0];
// Parse schemas from the second argument or default to 'public'
const schemas = args[1]
  ? args[1].split(',').map(schema => schema.trim())
  : ['public'];

console.log(`Connecting to database: ${databaseUrl}`);
console.log(`Using schemas: ${schemas.join(', ')}`);

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

const pool = new pg.Pool({
  connectionString: databaseUrl,
});

const SCHEMA_PATH = "schema";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    // Build a parameterized query for multiple schemas
    const placeholders = schemas.map((_, i) => `$${i + 1}`).join(', ');
    const query = `
      SELECT table_schema, table_name 
      FROM information_schema.tables 
      WHERE table_schema IN (${placeholders})
      ORDER BY table_schema, table_name
    `;

    const result = await client.query(query, schemas);

    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_schema}/${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" table in "${row.table_schema}" schema`,
      })),
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schemaPath = pathComponents.pop();
  const tableName = pathComponents.pop();
  const dbSchema = pathComponents.pop();

  if (schemaPath !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  if (!dbSchema) {
    throw new Error("Schema is required");
  }

  if (!schemas.includes(dbSchema)) {
    throw new Error(`Schema "${dbSchema}" is not in the allowed schemas list`);
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
      [dbSchema, tableName],
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "query",
        description: "Run a read-only SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "query") {
    // Ensure sql is provided
    if (!request.params.arguments || typeof request.params.arguments.sql !== 'string') {
      throw new Error("SQL query is required and must be a string");
    }

    const sql = request.params.arguments.sql;

    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION READ ONLY");
      const result = await client.query(sql);
      return {
        content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error),
        );

      client.release();
    }
  }
  throw new Error(`Unknown tool: ${request.params.name}`);
});

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch(console.error);
