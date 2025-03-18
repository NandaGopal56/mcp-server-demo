import sys
import os
import traceback
from typing import Any, List, Dict, Optional
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

class DatabaseConnector:
    """
    Handles database connection and provides methods for database introspection.
    This class encapsulates all database-related functionality.
    """
    
    def __init__(self):
        load_dotenv()
        """Initialize the database connector with connection parameters from environment variables."""
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.dbname = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.conn = None
    
    def connect(self) -> bool:
        """
        Establish a connection to the PostgreSQL database.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
            
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            return True
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            traceback.print_exc()
            return False
    
    def disconnect(self) -> None:
        """Close the database connection if it exists."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None
    
    def is_connected(self) -> bool:
        """
        Check if the database connection is active.
        
        Returns:
            bool: True if connected, False otherwise.
        """
        return self.conn is not None and not self.conn.closed
    
    def get_tables(self, schema_name: str = "public") -> Dict[str, Any]:
        """
        Get a list of all tables in the specified schema.
        
        Args:
            schema_name (str, optional): Schema name. Defaults to "public".
            
        Returns:
            Dict[str, Any]: Dictionary containing table names or error message.
        """
        if not self.is_connected():
            return {"error": "No active database connection"}
        
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name
            """, (schema_name,))
            
            tables = [row["table_name"] for row in cursor.fetchall()]
            cursor.close()
            
            return {
                "schema": schema_name,
                "tables": tables,
                "count": len(tables)
            }
        except Exception as e:
            error_msg = f"Error fetching tables: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            return {"error": error_msg}

    def get_table_schema(self, table_name: str, schema_name: str = "public") -> Dict[str, Any]:
        """
        Get the schema information for a specific table.
        
        Args:
            table_name (str): Name of the table
            schema_name (str, optional): Schema name. Defaults to "public".
            
        Returns:
            Dict[str, Any]: Dictionary containing column information or error message.
        """
        if not self.is_connected():
            return {"error": "No active database connection"}
        
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            
            columns = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            return {
                "schema": schema_name,
                "table": table_name,
                "columns": columns,
                "count": len(columns)
            }
        except Exception as e:
            error_msg = f"Error fetching table schema: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            return {"error": error_msg}
    
    
    def execute_query(self, query: str, params: List[Any] = None) -> Dict[str, Any]:
        """
        Execute a custom SQL query with optional parameters (SELECT queries only).
        
        Args:
            query (str): SQL query to execute (must be SELECT).
            params (List[Any], optional): Query parameters. Defaults to None.
            
        Returns:
            Dict[str, Any]: Dictionary containing query results or error message.
        """
        if not self.is_connected():
            return {"error": "No active database connection"}
        
        if not query.strip().lower().startswith("select"):
            return {"error": "Only SELECT queries are allowed for security reasons"}
        
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            
            return {
                "columns": columns,
                "rows": results,
                "row_count": len(results)
            }
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            return {"error": error_msg}


class DatabaseAnalyzer:
    """
    Main class for the MCP server that provides database analysis tools.
    """
    
    def __init__(self):
        """Initialize the DatabaseAnalyzer with MCP server and database connector."""

        self.mcp = FastMCP("database_analyzer")
        self.db = DatabaseConnector()
        self.db.connect()
        self._register_tools()
    
    def _register_tools(self):
        """Register MCP tools for database operations."""
        
        @self.mcp.tool()
        async def get_tables(schema_name: str = "public") -> Dict[str, Any]:
            """
            Get a list of all tables in the specified schema.
            
            Args:
                schema_name (str, optional): Schema name. Defaults to "public".
            """
            return self.db.get_tables(schema_name)
        
        @self.mcp.tool()
        async def get_table_schema(table_name: str, schema_name: str = "public") -> Dict[str, Any]:
            """
            Get the schema information for a specific table.
            
            Args:
                table_name (str): Name of the table
                schema_name (str, optional): Schema name. Defaults to "public".
            """
            return self.db.get_table_schema(table_name, schema_name)
        
        @self.mcp.tool()
        async def execute_query(query: str, params: List[Any] = None) -> Dict[str, Any]:
            """
            Execute a custom SQL query with optional parameters (SELECT queries only).
            
            Args:
                query (str): SQL query to execute (must be SELECT).
                params (List[Any], optional): Query parameters. Defaults to None.
            """
            return self.db.execute_query(query, params)
    
    def run(self):
        """Start the MCP server and handle cleanup on exit."""
        try:
            print("Running Database Analyzer MCP Server...")
            self.mcp.run(transport="stdio")
        except Exception as e:
            print(f"Fatal Error in MCP Server: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.db.disconnect()
            print("Database connection closed")


if __name__ == "__main__":
    analyzer = DatabaseAnalyzer()
    analyzer.run()