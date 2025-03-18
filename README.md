# Database Analyzer MCP Server

A powerful database analysis tool that provides an MCP (Model Context Protocol) server interface for PostgreSQL database introspection and analysis. This tool allows you to explore database schemas, relationships, and execute safe queries through a standardized interface.

## Features

- **Database Schema Analysis**

  - List all tables in a specified schema
  - Get detailed schema information for specific tables
  - View column definitions, data types, and constraints

- **Relationship Analysis**

  - Discover foreign key relationships between tables
  - View detailed relationship information including source and target tables/columns

- **Safe Query Execution**
  - Execute SELECT queries with parameterized inputs
  - Built-in security measures to prevent unauthorized modifications

## Prerequisites

- Python 3.x
- PostgreSQL database
- Required Python packages:
  - `mcp` (Model Context Protocol)
  - `psycopg2` (PostgreSQL adapter)
  - `python-dotenv` (Environment variable management)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd MCP-server-demo
```

2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your database credentials:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database_name
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

## Usage

1. Start the MCP server:

```bash
python server.py
```

2. The server provides the following tools:

## Security Features

- Only SELECT queries are allowed for security reasons
- Parameterized queries to prevent SQL injection
- Environment variable-based configuration for sensitive data
- Automatic connection cleanup and resource management

## Error Handling

The server includes comprehensive error handling for:

- Database connection issues
- Invalid queries
- Schema access problems
- Resource cleanup