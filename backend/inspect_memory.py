"""
Inspect all data stored in Cognee's SQLite + vector database.
Run: python inspect_memory.py
"""
import sqlite3, json, os, asyncio
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

DB_BASE = Path(
    r"C:\Users\ushan\AppData\Local\Programs\Python\Python314"
    r"\Lib\site-packages\cognee\.cognee_system\databases"
)

# ── 1. SQLite — relational store ───────────────────────────────
def inspect_sqlite():
    db_path = DB_BASE / "cognee_db"
    if not db_path.exists():
        print("SQLite DB not found:", db_path)
        return

    print("\n" + "═" * 70)
    print("  COGNEE SQLite DATABASE")
    print("═" * 70)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # List all tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print(f"\nTables found: {tables}\n")

    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM \"{table}\"")
            count = cur.fetchone()[0]
            print(f"── Table: {table}  ({count} rows)")

            if count == 0:
                continue

            # Show column names
            cur.execute(f"PRAGMA table_info(\"{table}\")")
            cols = [c[1] for c in cur.fetchall()]
            print(f"   Columns: {cols}")

            # Show first 5 rows
            cur.execute(f"SELECT * FROM \"{table}\" LIMIT 5")
            rows = cur.fetchall()
            for i, row in enumerate(rows):
                row_dict = dict(row)
                # Truncate long text fields
                for k, v in row_dict.items():
                    if isinstance(v, str) and len(v) > 120:
                        row_dict[k] = v[:120] + "…"
                print(f"   Row {i+1}: {json.dumps(row_dict, default=str, ensure_ascii=False)}")
            print()
        except Exception as e:
            print(f"   Error reading {table}: {e}\n")

    conn.close()


# ── 2. Cache SQLite — session/cache store ─────────────────────
def inspect_cache():
    db_path = DB_BASE / "cache.db"
    if not db_path.exists():
        print("Cache DB not found")
        return

    print("\n" + "═" * 70)
    print("  COGNEE CACHE DATABASE (session memory)")
    print("═" * 70)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print(f"\nTables: {tables}\n")

    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM \"{table}\"")
            count = cur.fetchone()[0]
            print(f"── Table: {table}  ({count} rows)")
            if count == 0:
                continue
            cur.execute(f"SELECT * FROM \"{table}\" LIMIT 5")
            rows = cur.fetchall()
            for i, row in enumerate(rows):
                row_dict = dict(row)
                for k, v in row_dict.items():
                    if isinstance(v, str) and len(v) > 120:
                        row_dict[k] = v[:120] + "…"
                print(f"   Row {i+1}: {json.dumps(row_dict, default=str, ensure_ascii=False)}")
            print()
        except Exception as e:
            print(f"   Error: {e}\n")

    conn.close()


# ── 3. Cognee recall() — query the live graph ─────────────────
async def query_live_graph():
    print("\n" + "═" * 70)
    print("  LIVE COGNEE GRAPH — recall() queries")
    print("═" * 70)

    import cognee
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.environ.get("GROQ_API_KEY", "")
    os.environ["LLM_PROVIDER"] = "groq"
    os.environ["LLM_MODEL"]    = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
    os.environ["LLM_API_KEY"]  = api_key
    os.environ.setdefault("EMBEDDING_PROVIDER", "local")

    case_id = os.getenv("CASE_ID", "case-2024-alpha-7")

    queries = [
        "All statements made by Marcus Harlow",
        "Contradictions detected in the case",
        "Location claims and alibis",
    ]

    for q in queries:
        print(f"\nQuery: \"{q}\"")
        print("─" * 50)
        try:
            results = await asyncio.wait_for(
                cognee.recall(query_text=q, datasets=[case_id], top_k=5),
                timeout=15.0,
            )
            if not results:
                print("  (no results)")
                continue
            for i, r in enumerate(results):
                source = getattr(r, "source", "?")
                if source == "graph":
                    text = getattr(r, "text", str(r))
                elif source == "session":
                    text = getattr(r, "answer", str(r))
                else:
                    text = str(r)
                print(f"  [{i+1}] source={source} | {str(text)[:200]}")
        except asyncio.TimeoutError:
            print("  (recall timed out)")
        except Exception as e:
            print(f"  (recall error: {e})")


# ── Main ───────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🔍 COGNEE MEMORY INSPECTOR")
    print("Case:", os.getenv("CASE_ID", "case-2024-alpha-7"))

    inspect_sqlite()
    inspect_cache()

    print("\n" + "═" * 70)
    print("  Running live recall() queries against the graph...")
    print("═" * 70)
    asyncio.run(query_live_graph())

    print("\n✓ Inspection complete.")
