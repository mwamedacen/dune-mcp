# Dune MCP Server

A comprehensive MCP (Model Context Protocol) server for interacting with [Dune Analytics](https://dune.com/) - the leading platform for blockchain data analysis.

## Features

### Tools

This server exposes all major Dune Analytics API endpoints as MCP tools:

#### SQL Execution
- `execute_sql` - Execute raw SQL queries against Dune's data engine
- `get_execution_status` - Check query execution status
- `get_execution_results` - Retrieve query results (JSON)
- `get_execution_results_csv` - Retrieve query results (CSV)
- `cancel_execution` - Cancel a running query

#### Saved Query Management
- `execute_query` - Execute a saved query by ID
- `get_query` - Get query details
- `get_query_results` - Get latest cached results without re-executing
- `get_query_results_csv` - Get latest results as CSV
- `create_query` - Create and save a new query
- `update_query` - Update an existing query
- `archive_query` - Archive a query
- `make_query_private` / `make_query_public` - Change query visibility

#### Data Upload
- `upload_csv` - Upload CSV data to create/update a table
- `create_table` - Create a new table with schema
- `insert_table_rows` - Insert rows into an existing table
- `clear_table` - Clear all data from a table
- `delete_table` - Permanently delete a table

### Resources

The server exposes comprehensive documentation as MCP resources to help LLMs write valid DuneSQL queries:

| Resource URI | Description |
|--------------|-------------|
| `dune://guide/sql-syntax` | DuneSQL (Trino) syntax reference, data types, functions |
| `dune://guide/tables` | Available tables: dex.trades, prices.usd, chain-specific tables |
| `dune://guide/query-patterns` | Common analytics patterns: volume, holders, whales, gas |
| `dune://guide/parameters` | How to use query parameters |
| `dune://guide/errors` | Common errors and troubleshooting |

## Installation

```bash
cd dune-mcp
pip install -e .
```

## Configuration

Set your Dune API key as an environment variable:

```bash
export DUNE_API_KEY="your-api-key-here"
```

Get your API key from [Dune Settings](https://dune.com/settings/api).

## Usage

### Running the Server

```bash
# Direct execution
python server.py

# Or via installed script
dune-mcp
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "dune": {
      "command": "python",
      "args": ["/path/to/dune-mcp/server.py"],
      "env": {
        "DUNE_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

Or using uv:

```json
{
  "mcpServers": {
    "dune": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/dune-mcp", "python", "server.py"],
      "env": {
        "DUNE_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

## Example Queries

### Simple DEX Volume Query

```python
# Using execute_sql tool
result = await execute_sql(
    sql="""
    SELECT 
        project,
        SUM(amount_usd) as volume
    FROM dex.trades
    WHERE block_time > now() - interval '24' hour
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
    """,
    performance="medium"
)
```

### Cross-Chain Analysis

```python
result = await execute_sql(
    sql="""
    SELECT 
        blockchain,
        COUNT(*) as tx_count,
        SUM(amount_usd) as volume_usd
    FROM dex.trades
    WHERE block_time > now() - interval '7' day
    GROUP BY 1
    ORDER BY 3 DESC
    """
)
```

### Create Parameterized Query

```python
result = await create_query(
    name="Token Volume Analysis",
    query_sql="""
    SELECT 
        DATE_TRUNC('day', block_time) as day,
        SUM(amount_usd) as volume
    FROM dex.trades
    WHERE token_bought_symbol = '{{token}}'
        AND block_time > now() - interval '{{days}}' day
    GROUP BY 1
    ORDER BY 1
    """,
    parameters=[
        {"key": "token", "value": "UNI", "type": "text"},
        {"key": "days", "value": "30", "type": "number"}
    ]
)
```

## Available Tables

### Curated (Cross-Chain)
- `dex.trades` - DEX trades across all EVM chains
- `dex_solana.trades` - Solana DEX trades
- `nft.trades` - NFT trades
- `prices.usd` - Token prices
- `tokens.erc20` - ERC20 token metadata

### Chain-Specific
- `ethereum.transactions`, `ethereum.logs`, `ethereum.traces`
- `polygon.transactions`, `polygon.logs`, etc.
- `arbitrum.transactions`, `optimism.transactions`, `base.transactions`

### Decoded Protocol Tables
- `uniswap_v3_ethereum.Pair_evt_Swap`
- `aave_v3_ethereum.Pool_evt_Supply`
- And many more...

## API Reference

See [Dune API Documentation](https://docs.dune.com/api-reference) for complete API details.

## License

MIT
