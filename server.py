"""
Dune Analytics MCP Server

A comprehensive MCP server for interacting with Dune Analytics API.
Provides tools for executing SQL queries, managing queries, uploading data,
and resources for SQL query guidance.
"""

import os
import httpx
from typing import Optional, Any
from fastmcp import FastMCP

# Initialize the MCP server
mcp = FastMCP(
    name="dune-mcp",
    instructions="""
    This MCP server provides access to Dune Analytics for blockchain data analysis.
    
    IMPORTANT: Before writing SQL queries, read the SQL guide resources to understand:
    - Available tables and schemas
    - Correct SQL syntax for Dune (Trino/DuneSQL)
    - Common query patterns for blockchain data
    
    Key resources to read first:
    - dune://guide/sql-syntax - SQL syntax reference
    - dune://guide/tables - Available tables and schemas
    - dune://guide/query-patterns - Common query patterns
    """
)

# Configuration
DUNE_API_BASE = "https://api.dune.com/api/v1"


def get_api_key() -> str:
    """Get the Dune API key from environment."""
    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        raise ValueError("DUNE_API_KEY environment variable is required")
    return api_key


def get_headers() -> dict:
    """Get headers for API requests."""
    return {
        "X-DUNE-API-KEY": get_api_key(),
        "Content-Type": "application/json"
    }


async def make_request(
    method: str,
    endpoint: str,
    json_data: Optional[dict] = None,
    params: Optional[dict] = None
) -> dict:
    """Make an HTTP request to the Dune API."""
    url = f"{DUNE_API_BASE}{endpoint}"
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.request(
            method=method,
            url=url,
            headers=get_headers(),
            json=json_data,
            params=params
        )
        response.raise_for_status()
        
        # Handle CSV responses
        if "csv" in endpoint:
            return {"csv_data": response.text}
        
        return response.json()


# =============================================================================
# SQL EXECUTION TOOLS
# =============================================================================

@mcp.tool
async def execute_sql(
    sql: str,
    performance: str = "medium"
) -> dict:
    """
    Execute a raw SQL query against Dune's data engine.
    
    This is the primary tool for running custom SQL queries on blockchain data.
    Returns an execution_id that can be used to check status and retrieve results.
    
    Args:
        sql: The SQL query to execute. Use DuneSQL (Trino) syntax.
             Read dune://guide/sql-syntax for syntax reference.
        performance: Performance tier - "medium" (default) or "large" for complex queries.
    
    Returns:
        Execution details including execution_id and initial state.
        
    Example SQL queries:
        - SELECT * FROM dex.trades WHERE block_time > now() - interval '1' day LIMIT 10
        - SELECT blockchain, SUM(amount_usd) as volume FROM dex.trades GROUP BY 1
    """
    return await make_request(
        "POST",
        "/sql/execute",
        json_data={"sql": sql, "performance": performance}
    )


@mcp.tool
async def get_execution_status(execution_id: str) -> dict:
    """
    Check the status of a query execution.
    
    Use this to poll for completion after executing a query.
    
    Args:
        execution_id: The execution ID returned from execute_sql or execute_query.
    
    Returns:
        Execution status including state (QUERY_STATE_EXECUTING, QUERY_STATE_COMPLETED, etc.),
        queue position, and timing information.
    """
    return await make_request("GET", f"/execution/{execution_id}/status")


@mcp.tool
async def get_execution_results(
    execution_id: str,
    limit: int = 100,
    offset: int = 0
) -> dict:
    """
    Retrieve the results of a completed query execution.
    
    Args:
        execution_id: The execution ID from a completed query.
        limit: Maximum number of rows to return (default 100, max per page).
        offset: Row offset for pagination (default 0).
    
    Returns:
        Query results including rows, column metadata, and pagination info.
    """
    params = {"limit": limit, "offset": offset}
    return await make_request("GET", f"/execution/{execution_id}/results", params=params)


@mcp.tool
async def get_execution_results_csv(
    execution_id: str,
    allow_partial_results: bool = False
) -> dict:
    """
    Retrieve query execution results in CSV format.
    
    Args:
        execution_id: The execution ID from a completed query.
        allow_partial_results: Allow truncated results if data exceeds 8GB.
    
    Returns:
        Query results as CSV string.
    """
    params = {"allow_partial_results": str(allow_partial_results).lower()}
    return await make_request("GET", f"/execution/{execution_id}/results/csv", params=params)


@mcp.tool
async def cancel_execution(execution_id: str) -> dict:
    """
    Cancel an ongoing query execution.
    
    Args:
        execution_id: The execution ID of the running query.
    
    Returns:
        Success status of the cancellation.
    """
    return await make_request("POST", f"/execution/{execution_id}/cancel")


# =============================================================================
# SAVED QUERY TOOLS
# =============================================================================

@mcp.tool
async def execute_query(
    query_id: int,
    query_parameters: Optional[dict] = None,
    performance: str = "medium"
) -> dict:
    """
    Execute a saved query by its ID.
    
    Args:
        query_id: The unique identifier of the saved query.
        query_parameters: Optional parameters to pass to the query (key-value pairs).
        performance: Performance tier - "medium" (default) or "large".
    
    Returns:
        Execution details including execution_id.
    """
    payload = {"performance": performance}
    if query_parameters:
        payload["query_parameters"] = query_parameters
    
    return await make_request("POST", f"/query/{query_id}/execute", json_data=payload)


@mcp.tool
async def get_query(query_id: int) -> dict:
    """
    Retrieve details about a saved query.
    
    Args:
        query_id: The unique identifier of the query.
    
    Returns:
        Query details including SQL, parameters, name, tags, and state.
    """
    return await make_request("GET", f"/query/{query_id}")


@mcp.tool
async def get_query_results(
    query_id: int,
    limit: int = 100,
    offset: int = 0,
    allow_partial_results: bool = False
) -> dict:
    """
    Get the latest results of a saved query without re-executing.
    
    This retrieves cached results from the most recent execution.
    Does not trigger a new execution but consumes credits.
    
    Args:
        query_id: The unique identifier of the query.
        limit: Maximum rows to return.
        offset: Row offset for pagination.
        allow_partial_results: Allow truncated results if data is too large.
    
    Returns:
        Latest query results in JSON format.
    """
    params = {
        "limit": limit,
        "offset": offset,
        "allow_partial_results": str(allow_partial_results).lower()
    }
    return await make_request("GET", f"/query/{query_id}/results", params=params)


@mcp.tool
async def get_query_results_csv(
    query_id: int,
    allow_partial_results: bool = False,
    columns: Optional[str] = None,
    sort_by: Optional[str] = None,
    filters: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> dict:
    """
    Get the latest results of a saved query in CSV format.
    
    Args:
        query_id: The unique identifier of the query.
        allow_partial_results: Allow truncated results if data exceeds limit.
        columns: Comma-separated list of column names to return.
        sort_by: SQL ORDER BY expression (e.g., "volume DESC").
        filters: SQL WHERE clause expression for filtering rows.
        limit: Maximum number of rows to return.
        offset: Row offset for pagination.
    
    Returns:
        Query results as CSV string.
    """
    params = {"allow_partial_results": str(allow_partial_results).lower()}
    if columns:
        params["columns"] = columns
    if sort_by:
        params["sort_by"] = sort_by
    if filters:
        params["filters"] = filters
    if limit:
        params["limit"] = limit
    if offset:
        params["offset"] = offset
    
    return await make_request("GET", f"/query/{query_id}/results/csv", params=params)


@mcp.tool
async def create_query(
    name: str,
    query_sql: str,
    description: str = "",
    is_private: bool = False,
    parameters: Optional[list] = None,
    tags: Optional[list] = None
) -> dict:
    """
    Create and save a new query on Dune.
    
    Args:
        name: Name for the query.
        query_sql: The SQL query text. Use {{param_name}} for parameters.
        description: Optional description of what the query does.
        is_private: Whether the query should be private (default False).
        parameters: Optional list of parameter definitions, each with:
            - key: Parameter name
            - value: Default value
            - type: "text", "number", or "enum"
            - enumOptions: List of allowed values (for enum type)
        tags: Optional list of tags for organization.
    
    Returns:
        Created query details including query_id.
        
    Example:
        create_query(
            name="Top DEX Volume",
            query_sql="SELECT project, SUM(amount_usd) as volume FROM dex.trades WHERE block_time > now() - interval '{{days}}' day GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            parameters=[{"key": "days", "value": "7", "type": "number"}]
        )
    """
    payload = {
        "name": name,
        "query_sql": query_sql,
        "description": description,
        "is_private": is_private
    }
    if parameters:
        payload["parameters"] = parameters
    if tags:
        payload["tags"] = tags
    
    return await make_request("POST", "/query", json_data=payload)


@mcp.tool
async def update_query(
    query_id: int,
    query_sql: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[list] = None,
    tags: Optional[list] = None,
    is_private: Optional[bool] = None
) -> dict:
    """
    Update an existing saved query.
    
    Only fields provided will be updated.
    
    Args:
        query_id: The unique identifier of the query to update.
        query_sql: New SQL query text.
        name: New name for the query.
        description: New description.
        parameters: New parameter definitions.
        tags: New tags list.
        is_private: Change privacy setting.
    
    Returns:
        Update confirmation.
    """
    payload = {"query_id": query_id}
    if query_sql is not None:
        payload["query_sql"] = query_sql
    if name is not None:
        payload["query_name"] = name
    if description is not None:
        payload["description"] = description
    if parameters is not None:
        payload["parameters"] = parameters
    if tags is not None:
        payload["query_tags"] = tags
    if is_private is not None:
        payload["is_public"] = not is_private
    
    return await make_request("PATCH", f"/query/{query_id}", json_data=payload)


@mcp.tool
async def archive_query(query_id: int) -> dict:
    """
    Archive a query, making it uneditable and unexecutable.
    
    Args:
        query_id: The unique identifier of the query to archive.
    
    Returns:
        Archive confirmation.
    """
    return await make_request("POST", f"/query/{query_id}/archive")


@mcp.tool
async def make_query_private(query_id: int) -> dict:
    """
    Make a query private, restricting access to the owner.
    
    Args:
        query_id: The unique identifier of the query.
    
    Returns:
        Privacy change confirmation.
    """
    return await make_request("PATCH", f"/query/{query_id}/private")


@mcp.tool
async def make_query_public(query_id: int) -> dict:
    """
    Make a private query public, allowing broader access.
    
    Args:
        query_id: The unique identifier of the query.
    
    Returns:
        Privacy change confirmation.
    """
    return await make_request("PATCH", f"/query/{query_id}/unprivate")


# =============================================================================
# DATA UPLOAD TOOLS
# =============================================================================

@mcp.tool
async def upload_csv(
    table_name: str,
    data: str,
    description: str = "",
    is_private: bool = False
) -> dict:
    """
    Upload CSV data to create or overwrite a table in Dune.
    
    Maximum file size is 200MB. Uploading to an existing table overwrites all data.
    
    Args:
        table_name: Name for the table (will be accessible as dune.your_namespace.table_name).
        data: CSV data as string, including headers.
        description: Optional description of the data.
        is_private: Whether the table should be private.
    
    Returns:
        Upload confirmation.
        
    Example:
        upload_csv(
            table_name="my_token_prices",
            data="date,token,price\\n2024-01-01,ETH,2500\\n2024-01-02,ETH,2600"
        )
    """
    return await make_request(
        "POST",
        "/table/upload/csv",
        json_data={
            "table_name": table_name,
            "data": data,
            "description": description,
            "is_private": is_private
        }
    )


@mcp.tool
async def create_table(
    namespace: str,
    table_name: str,
    columns: list,
    is_public: bool = False
) -> dict:
    """
    Create a new table with a defined schema.
    
    Args:
        namespace: Namespace for the table (usually your username).
        table_name: Name for the table.
        columns: List of column definitions, each with:
            - name: Column name
            - type: Data type ("string", "integer", "double", "timestamp", "boolean")
            - nullable: Whether nulls are allowed (default True)
        is_public: Whether the table is publicly accessible.
    
    Returns:
        Table creation confirmation.
        
    Example:
        create_table(
            namespace="my_namespace",
            table_name="token_metrics",
            columns=[
                {"name": "date", "type": "timestamp", "nullable": False},
                {"name": "token", "type": "string"},
                {"name": "volume", "type": "double"}
            ]
        )
    """
    return await make_request(
        "POST",
        "/uploads",
        json_data={
            "namespace": namespace,
            "table_name": table_name,
            "columns": columns,
            "is_public": is_public
        }
    )


@mcp.tool
async def insert_table_rows(
    namespace: str,
    table_name: str,
    rows: list
) -> dict:
    """
    Insert rows into an existing table.
    
    Args:
        namespace: Namespace of the table.
        table_name: Name of the table.
        rows: List of row objects where keys match column names.
    
    Returns:
        Insert confirmation with row count.
        
    Example:
        insert_table_rows(
            namespace="my_namespace",
            table_name="token_metrics",
            rows=[
                {"date": "2024-01-01T00:00:00Z", "token": "ETH", "volume": 1000000},
                {"date": "2024-01-02T00:00:00Z", "token": "ETH", "volume": 1200000}
            ]
        )
    """
    return await make_request(
        "POST",
        f"/uploads/{namespace}/{table_name}/insert",
        json_data={"rows": rows}
    )


@mcp.tool
async def clear_table(namespace: str, table_name: str) -> dict:
    """
    Remove all data from a table while preserving its schema.
    
    Args:
        namespace: Namespace of the table.
        table_name: Name of the table to clear.
    
    Returns:
        Clear confirmation.
    """
    return await make_request("POST", f"/uploads/{namespace}/{table_name}/clear")


@mcp.tool
async def delete_table(namespace: str, table_name: str) -> dict:
    """
    Permanently delete a table and all its data.
    
    WARNING: This operation is irreversible!
    
    Args:
        namespace: Namespace of the table.
        table_name: Name of the table to delete.
    
    Returns:
        Deletion confirmation.
    """
    return await make_request("DELETE", f"/uploads/{namespace}/{table_name}")


# =============================================================================
# RESOURCES - SQL GUIDE AND DOCUMENTATION
# =============================================================================

@mcp.resource("dune://guide/sql-syntax")
def get_sql_syntax_guide() -> str:
    """DuneSQL syntax reference and best practices."""
    return """
# DuneSQL Syntax Guide

DuneSQL is based on Trino (formerly PrestoSQL). Here are the key syntax rules:

## Basic Query Structure
```sql
SELECT column1, column2, aggregate_function(column3)
FROM schema.table
WHERE condition
GROUP BY column1, column2
HAVING aggregate_condition
ORDER BY column1 DESC
LIMIT 100
```

## Data Types
- VARCHAR / STRING: Text data
- INTEGER / BIGINT: Whole numbers
- DOUBLE / DECIMAL: Decimal numbers  
- BOOLEAN: true/false
- TIMESTAMP: Date and time
- DATE: Date only
- VARBINARY: Binary data (for addresses, hashes)

## Time Functions (CRITICAL for blockchain queries)
```sql
-- Current time
now()
current_timestamp

-- Time intervals (USE QUOTES!)
WHERE block_time > now() - interval '24' hour
WHERE block_time > now() - interval '7' day
WHERE block_time > now() - interval '1' month

-- Date truncation
DATE_TRUNC('day', block_time)
DATE_TRUNC('hour', block_time)
DATE_TRUNC('week', block_time)
DATE_TRUNC('month', block_time)

-- Date extraction
EXTRACT(year FROM block_time)
EXTRACT(month FROM block_time)
EXTRACT(day FROM block_time)
```

## Address Handling
```sql
-- Addresses are stored as VARBINARY, convert for display
SELECT 
    CAST(address AS VARCHAR) as address_string,
    -- Or use Dune's helper
    '0x' || CAST(address AS VARCHAR) as formatted_address
FROM table

-- Comparing addresses (use lowercase, 0x prefix)
WHERE "from" = 0x1234...  -- Use 0x prefix, no quotes for comparison
WHERE CAST(address AS VARCHAR) = '1234...'  -- Without 0x for cast comparison
```

## Aggregation Functions
```sql
COUNT(*)                    -- Count rows
COUNT(DISTINCT column)      -- Count unique values
SUM(amount)                 -- Sum values
AVG(amount)                 -- Average
MIN(amount), MAX(amount)    -- Min/Max
APPROX_DISTINCT(column)     -- Fast approximate count distinct
APPROX_PERCENTILE(column, 0.5)  -- Median
```

## String Functions
```sql
CONCAT(str1, str2)
LOWER(string), UPPER(string)
SUBSTR(string, start, length)
LENGTH(string)
REPLACE(string, from, to)
SPLIT(string, delimiter)
```

## Array Functions
```sql
ARRAY_AGG(column)           -- Aggregate into array
CARDINALITY(array)          -- Array length
CONTAINS(array, element)    -- Check if contains
ARRAY_JOIN(array, ',')      -- Join array to string
```

## Conditional Logic
```sql
CASE 
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END

COALESCE(value1, value2, default)  -- First non-null value
NULLIF(value1, value2)             -- NULL if equal
IF(condition, true_value, false_value)
```

## Window Functions
```sql
ROW_NUMBER() OVER (ORDER BY column)
RANK() OVER (PARTITION BY col1 ORDER BY col2)
LAG(column, 1) OVER (ORDER BY time)    -- Previous row value
LEAD(column, 1) OVER (ORDER BY time)   -- Next row value
SUM(amount) OVER (PARTITION BY address ORDER BY time)  -- Running sum
```

## Common Table Expressions (CTEs)
```sql
WITH daily_volume AS (
    SELECT 
        DATE_TRUNC('day', block_time) as day,
        SUM(amount_usd) as volume
    FROM dex.trades
    WHERE block_time > now() - interval '30' day
    GROUP BY 1
),
weekly_avg AS (
    SELECT AVG(volume) as avg_volume
    FROM daily_volume
)
SELECT * FROM daily_volume, weekly_avg
```

## IMPORTANT NOTES
1. Always use LIMIT to prevent expensive queries
2. Filter on block_time first - it's indexed!
3. Use DATE_TRUNC for time-based grouping
4. Addresses are case-insensitive in comparisons
5. Use single quotes for strings, no quotes for 0x addresses
6. Interval syntax: interval '1' day (number in quotes!)
"""


@mcp.resource("dune://guide/tables")
def get_tables_guide() -> str:
    """Available Dune tables and schemas reference."""
    return """
# Dune Tables and Schemas Reference

## Curated Data Tables (Recommended!)

These are pre-processed, cross-chain tables maintained by Dune:

### DEX (Decentralized Exchange) Data
```sql
-- All DEX trades across EVM chains
dex.trades
  - blockchain        -- 'ethereum', 'polygon', 'arbitrum', etc.
  - project           -- 'uniswap', 'sushiswap', 'curve', etc.
  - version           -- Protocol version
  - block_time        -- Transaction timestamp
  - block_number      
  - token_bought_symbol
  - token_sold_symbol
  - token_bought_amount
  - token_sold_amount
  - amount_usd        -- USD value of trade
  - tx_hash
  - tx_from           -- Transaction sender
  - tx_to             -- Transaction recipient
  - taker             -- Trade taker address
  - maker             -- Trade maker address

-- Solana DEX trades
dex_solana.trades
  - Similar structure but for Solana

-- Example: Top DEX by volume last 24h
SELECT 
    project,
    SUM(amount_usd) as volume_usd,
    COUNT(*) as trade_count
FROM dex.trades
WHERE block_time > now() - interval '24' hour
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
```

### Token Data
```sql
-- ERC20 token metadata
tokens.erc20
  - blockchain
  - contract_address
  - symbol
  - decimals

-- Token prices (USD)
prices.usd
  - blockchain
  - contract_address
  - symbol
  - price
  - minute           -- Price timestamp (minute granularity)

-- Example: Get ETH price
SELECT price 
FROM prices.usd 
WHERE symbol = 'WETH' 
  AND blockchain = 'ethereum'
  AND minute > now() - interval '1' hour
ORDER BY minute DESC
LIMIT 1
```

### NFT Data
```sql
-- NFT trades
nft.trades
  - blockchain
  - project           -- 'opensea', 'blur', 'looksrare', etc.
  - nft_contract_address
  - token_id
  - amount_usd
  - buyer
  - seller
  - block_time
```

### Transfers
```sql
-- ERC20 transfers
erc20_<chain>.evt_Transfer
  - contract_address
  - "from"
  - "to"  
  - value
  - evt_block_time

-- Native token transfers (ETH, MATIC, etc.)
<chain>.traces
  - "from"
  - "to"
  - value
  - block_time
```

## Raw Blockchain Tables

### Ethereum (and other EVM chains)
Replace `ethereum` with: polygon, arbitrum, optimism, bnb, avalanche_c, gnosis, fantom, base, zksync, etc.

```sql
-- Blocks
ethereum.blocks
  - number
  - hash
  - time
  - miner
  - gas_used
  - gas_limit
  - base_fee_per_gas

-- Transactions
ethereum.transactions
  - hash
  - block_number
  - block_time
  - "from"
  - "to"
  - value              -- Native token amount (in wei)
  - gas_price
  - gas_used
  - success
  - data               -- Input data

-- Logs (Events)
ethereum.logs
  - block_number
  - block_time
  - tx_hash
  - contract_address
  - topic0             -- Event signature
  - topic1, topic2, topic3  -- Indexed parameters
  - data               -- Non-indexed parameters

-- Internal transactions
ethereum.traces
  - block_time
  - tx_hash
  - "from"
  - "to"
  - value
  - type               -- 'call', 'create', 'delegatecall', etc.
  - success
```

### Decoded Tables (Protocol-Specific)
Dune decodes contract events and calls into readable tables:

```sql
-- Uniswap V3 swaps
uniswap_v3_ethereum.Pair_evt_Swap
  - evt_block_time
  - sender
  - recipient
  - amount0
  - amount1
  - sqrtPriceX96
  - tick
  - contract_address

-- ERC20 Transfer events
erc20_ethereum.evt_Transfer
  - evt_block_time
  - contract_address
  - "from"
  - "to"
  - value

-- Generic pattern: <project>_<chain>.<Contract>_evt_<Event>
```

### Solana Tables
```sql
-- Solana transactions
solana.transactions
  - block_time
  - block_slot
  - signature
  - success
  - fee
  - signer

-- Solana account activity
solana.account_activity
  - block_time
  - address
  - tx_signature
  - balance_change
```

## User-Uploaded Tables
```sql
-- Your uploaded data
dune.<your_namespace>.<table_name>
```

## Tips for Finding Tables
1. Use dex.trades, nft.trades for aggregated data first
2. Look for decoded tables: <protocol>_<chain>.<Contract>_evt_<Event>
3. Raw data in <chain>.transactions, <chain>.logs
4. Check Dune's data explorer for schema details
"""


@mcp.resource("dune://guide/query-patterns")
def get_query_patterns() -> str:
    """Common query patterns for blockchain analytics."""
    return """
# Common Query Patterns for Blockchain Analytics

## 1. Time-Series Volume Analysis
```sql
-- Daily DEX volume by chain
SELECT 
    blockchain,
    DATE_TRUNC('day', block_time) as day,
    SUM(amount_usd) as volume_usd,
    COUNT(*) as trade_count
FROM dex.trades
WHERE block_time > now() - interval '30' day
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC
```

## 2. Top Traders/Wallets
```sql
-- Top traders by volume
SELECT 
    tx_from as trader,
    COUNT(*) as trade_count,
    SUM(amount_usd) as total_volume,
    COUNT(DISTINCT DATE_TRUNC('day', block_time)) as active_days
FROM dex.trades
WHERE block_time > now() - interval '7' day
GROUP BY 1
ORDER BY 3 DESC
LIMIT 100
```

## 3. Token Holder Analysis
```sql
-- Current token holders (simplified)
WITH transfers AS (
    SELECT 
        "to" as address,
        SUM(CAST(value AS DOUBLE)) as received
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x... -- token address
    GROUP BY 1
),
sent AS (
    SELECT 
        "from" as address,
        SUM(CAST(value AS DOUBLE)) as sent
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x...
    GROUP BY 1
)
SELECT 
    COALESCE(t.address, s.address) as holder,
    COALESCE(t.received, 0) - COALESCE(s.sent, 0) as balance
FROM transfers t
FULL OUTER JOIN sent s ON t.address = s.address
WHERE COALESCE(t.received, 0) - COALESCE(s.sent, 0) > 0
ORDER BY 2 DESC
LIMIT 100
```

## 4. Protocol Revenue/Fees
```sql
-- DEX trading fees by protocol
SELECT 
    project,
    DATE_TRUNC('day', block_time) as day,
    SUM(amount_usd * 0.003) as estimated_fees  -- Assuming 0.3% fee
FROM dex.trades
WHERE block_time > now() - interval '30' day
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC
```

## 5. Cross-Chain Comparison
```sql
-- Compare activity across chains
SELECT 
    blockchain,
    COUNT(DISTINCT DATE_TRUNC('day', block_time)) as active_days,
    SUM(amount_usd) as total_volume,
    COUNT(*) as total_trades,
    COUNT(DISTINCT tx_from) as unique_traders
FROM dex.trades
WHERE block_time > now() - interval '30' day
GROUP BY 1
ORDER BY 3 DESC
```

## 6. New vs Returning Users
```sql
WITH first_trade AS (
    SELECT 
        tx_from as trader,
        MIN(DATE_TRUNC('day', block_time)) as first_day
    FROM dex.trades
    GROUP BY 1
)
SELECT 
    DATE_TRUNC('day', d.block_time) as day,
    COUNT(DISTINCT CASE WHEN f.first_day = DATE_TRUNC('day', d.block_time) THEN d.tx_from END) as new_users,
    COUNT(DISTINCT CASE WHEN f.first_day < DATE_TRUNC('day', d.block_time) THEN d.tx_from END) as returning_users
FROM dex.trades d
JOIN first_trade f ON d.tx_from = f.trader
WHERE d.block_time > now() - interval '30' day
GROUP BY 1
ORDER BY 1 DESC
```

## 7. Token Price with Volume
```sql
SELECT 
    DATE_TRUNC('hour', p.minute) as hour,
    AVG(p.price) as avg_price,
    SUM(t.amount_usd) as volume
FROM prices.usd p
LEFT JOIN dex.trades t 
    ON t.token_bought_symbol = p.symbol
    AND DATE_TRUNC('hour', t.block_time) = DATE_TRUNC('hour', p.minute)
WHERE p.symbol = 'UNI'
    AND p.blockchain = 'ethereum'
    AND p.minute > now() - interval '7' day
GROUP BY 1
ORDER BY 1
```

## 8. Whale Tracking
```sql
-- Large transfers
SELECT 
    block_time,
    tx_hash,
    "from",
    "to",
    CAST(value AS DOUBLE) / 1e18 as amount,  -- Assuming 18 decimals
    amount_usd
FROM dex.trades
WHERE amount_usd > 1000000  -- $1M+ trades
    AND block_time > now() - interval '24' hour
ORDER BY amount_usd DESC
```

## 9. Contract Interaction Analysis
```sql
-- Most called contracts
SELECT 
    "to" as contract,
    COUNT(*) as tx_count,
    COUNT(DISTINCT "from") as unique_callers
FROM ethereum.transactions
WHERE block_time > now() - interval '7' day
    AND success = true
    AND "to" IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20
```

## 10. Gas Analysis
```sql
-- Average gas prices by hour
SELECT 
    DATE_TRUNC('hour', block_time) as hour,
    AVG(gas_price / 1e9) as avg_gas_gwei,
    APPROX_PERCENTILE(gas_price / 1e9, 0.5) as median_gas_gwei,
    COUNT(*) as tx_count
FROM ethereum.transactions
WHERE block_time > now() - interval '24' hour
GROUP BY 1
ORDER BY 1
```

## Query Optimization Tips

1. **Always filter on block_time first** - it's indexed
2. **Use LIMIT** during development
3. **Avoid SELECT *** - specify columns
4. **Use CTEs** for complex queries (more readable)
5. **Use APPROX_DISTINCT** instead of COUNT(DISTINCT) for large datasets
6. **Filter before joining** to reduce data volume
7. **Use curated tables** (dex.trades, prices.usd) when possible
"""


@mcp.resource("dune://guide/parameters")
def get_parameters_guide() -> str:
    """How to use query parameters in Dune."""
    return """
# Query Parameters in Dune

Parameters allow you to create reusable, dynamic queries.

## Parameter Syntax
Use double curly braces: {{parameter_name}}

```sql
SELECT *
FROM dex.trades
WHERE blockchain = '{{chain}}'
    AND block_time > now() - interval '{{days}}' day
    AND amount_usd > {{min_amount}}
LIMIT {{limit}}
```

## Parameter Types

### Text Parameters
For strings like addresses, symbols, chain names:
```json
{
    "key": "chain",
    "value": "ethereum",
    "type": "text"
}
```

### Number Parameters
For numeric values:
```json
{
    "key": "days",
    "value": "7",
    "type": "number"
}
```

### Enum Parameters
For dropdown selections:
```json
{
    "key": "chain",
    "value": "ethereum",
    "type": "enum",
    "enumOptions": ["ethereum", "polygon", "arbitrum", "optimism", "base"]
}
```

## Creating Parameterized Queries via API

```python
create_query(
    name="DEX Volume by Chain",
    query_sql=\"\"\"
        SELECT 
            DATE_TRUNC('day', block_time) as day,
            SUM(amount_usd) as volume
        FROM dex.trades
        WHERE blockchain = '{{chain}}'
            AND block_time > now() - interval '{{days}}' day
        GROUP BY 1
        ORDER BY 1
    \"\"\",
    parameters=[
        {
            "key": "chain",
            "value": "ethereum",
            "type": "enum",
            "enumOptions": ["ethereum", "polygon", "arbitrum", "optimism", "base", "bnb"]
        },
        {
            "key": "days",
            "value": "30",
            "type": "number"
        }
    ]
)
```

## Executing with Parameters

```python
execute_query(
    query_id=12345,
    query_parameters={
        "chain": "polygon",
        "days": 14
    }
)
```

## Best Practices

1. **Provide sensible defaults** - queries should work without parameters
2. **Use enums for fixed options** - prevents SQL injection, improves UX
3. **Document parameters** in query description
4. **Validate numbers** - ensure they make sense (positive, within range)
5. **Use text for addresses** - allows flexible input
"""


@mcp.resource("dune://guide/errors")
def get_errors_guide() -> str:
    """Common errors and troubleshooting for Dune queries."""
    return """
# Common Dune Query Errors and Solutions

## Syntax Errors

### "mismatched input" or "extraneous input"
**Cause**: SQL syntax error
**Fix**: Check for:
- Missing commas between columns
- Missing quotes around strings
- Wrong interval syntax: use `interval '1' day` not `interval 1 day`

### "Column 'x' cannot be resolved"
**Cause**: Column doesn't exist or wrong table
**Fix**: 
- Check column name spelling
- Verify you're querying the right table
- Use the data explorer to confirm schema

### "Table 'x' does not exist"
**Cause**: Wrong table name or schema
**Fix**:
- Check table path: schema.table_name
- For decoded tables: project_chain.Contract_evt_Event
- Use data explorer to find correct name

## Data Type Errors

### "Cannot cast" or type mismatch
**Cause**: Comparing incompatible types
**Fix**:
```sql
-- Wrong
WHERE address = '0x1234...'  -- comparing varbinary to string

-- Correct
WHERE address = 0x1234...  -- no quotes for hex addresses
-- Or
WHERE CAST(address AS VARCHAR) = '1234...'
```

### "Division by zero"
**Fix**: Use NULLIF
```sql
SELECT amount / NULLIF(total, 0) as ratio
```

## Performance Issues

### Query timeout
**Causes**: 
- No time filter
- Too much data
- Inefficient joins

**Fixes**:
```sql
-- Always filter on time first (indexed!)
WHERE block_time > now() - interval '7' day

-- Add LIMIT during development
LIMIT 1000

-- Use APPROX functions for large datasets
SELECT APPROX_DISTINCT(address) as unique_count
```

### "Query exceeded memory limit"
**Fixes**:
- Add more filters
- Use performance: "large" tier
- Break query into smaller parts using CTEs
- Avoid SELECT * 

## Common Mistakes

### Wrong interval syntax
```sql
-- WRONG
interval 7 day
interval '7 days'

-- CORRECT
interval '7' day
interval '24' hour
interval '1' month
```

### Forgetting to handle NULL
```sql
-- May exclude rows with NULL
WHERE amount > 0

-- Include NULL handling
WHERE COALESCE(amount, 0) > 0
```

### Case sensitivity in strings
```sql
-- May miss data
WHERE symbol = 'eth'

-- Better
WHERE LOWER(symbol) = 'eth'
```

### Aggregation without GROUP BY
```sql
-- WRONG: non-aggregated column without GROUP BY
SELECT blockchain, SUM(amount) FROM dex.trades

-- CORRECT
SELECT blockchain, SUM(amount) FROM dex.trades GROUP BY blockchain
```

## API-Specific Errors

### 401 Unauthorized
**Fix**: Check DUNE_API_KEY is set correctly

### 402 Payment Required  
**Fix**: Query exceeds plan limits - optimize or upgrade

### 403 Forbidden
**Fix**: Query is archived, private, or no permission

### 404 Not Found
**Fix**: Invalid query_id or execution_id

### 429 Rate Limited
**Fix**: Slow down requests, implement backoff
"""


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import sys

    # Check for HTTP mode via command line argument
    if "--http" in sys.argv:
        # Run as HTTP server (streamable-http transport)
        port = 8000
        for i, arg in enumerate(sys.argv):
            if arg == "--port" and i + 1 < len(sys.argv):
                port = int(sys.argv[i + 1])

        print(f"Starting Dune MCP server on http://127.0.0.1:{port}")
        mcp.run(transport="streamable-http", host="127.0.0.1", port=port)
    else:
        # Default: stdio transport (for Claude Desktop, Claude Code, etc.)
        mcp.run()
