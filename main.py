import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
import logging
from typing import Dict
import asyncpg# Replace with your actual DB type
from mcp.server.fastmcp import FastMCP, Context
from dotenv import load_dotenv
import os
from contextlib import asynccontextmanager


# Carrega as variáveis de ambiente
load_dotenv()

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuração do banco de dados
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_URL = f"postgres://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"


# Contexto da aplicação
@dataclass
class AppContext:
    db: asyncpg.Pool

# Pool de conexões
db_pool = None
async def get_db_pool() -> asyncpg.Pool:
    global db_pool
    if db_pool is None:
        try:
            logger.info(f"Conectando ao banco de dados: {DATABASE_URL}")
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("Conexão com o banco de dados estabelecida com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar com o banco de dados: {e}")
            raise
    return db_pool

@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    logger.info("Iniciando ciclo de vida do MCP...")
    db = await get_db_pool()
    logger.info("Pool de conexões pronto, servidor MCP inicializado e pronto para receber requisições.")
    try:
        yield AppContext(db=db)
    finally:
        logger.info("Encerrando ciclo de vida do MCP. Fechando pool de conexões...")
        await db.close()
   
# Inicializa o servidor MCP
mcp = FastMCP("PostgresMCP", lifespan=app_lifespan, port=3001)

@mcp.tool()
async def health_check(ctx: Context) -> Dict:
    db = ctx.request_context.lifespan_context.db
    try:
        async with db.acquire() as conn:
            await conn.execute("SELECT 1")
            
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Erro no health check: {e}")
        return {"status": "error", "error": "Service unavailable"}

@mcp.tool()
async def list_tables(ctx: Context) -> Dict:
    db = ctx.request_context.lifespan_context.db
    async with db.acquire() as conn:
        result = await conn.fetch("SELECT tablename FROM pg_tables where tablename not like 'pg_%' and tablename not like 'sql_%'")
        return [dict(row) for row in result]

@mcp.tool()
async def get_table_schema(ctx: Context, table_name: str) -> Dict:
    db = ctx.request_context.lifespan_context.db
    async with db.acquire() as conn:
        result = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
        return [dict(row) for row in result]

@mcp.tool()
async def execute_query(ctx: Context, query: str) -> Dict:
    db = ctx.request_context.lifespan_context.db
    async with db.acquire() as conn:
        logger.info(f"Executando query: {query}")
        result = await conn.fetch(query)
        return [dict(row) for row in result]

if __name__ == "__main__":
    logger.info("Iniciando servidor MCP...")
    mcp.run(
      transport='sse'
    )
