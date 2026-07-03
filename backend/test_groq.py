"""
Quick smoke test for the Groq connection.
Run:  python test_groq.py
"""
import os, asyncio
from dotenv import load_dotenv
load_dotenv()

async def main():
    key   = os.environ.get("GROQ_API_KEY", "")
    model = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")

    print(f"Key   : {key[:8]}...{key[-4:] if len(key) > 12 else 'TOO SHORT'}")
    print(f"Model : {model}")

    if not key or key.startswith("your_"):
        print("\n✗  GROQ_API_KEY not set in .env")
        print("   Get a free key at: https://console.groq.com/keys")
        return

    from langchain_groq import ChatGroq
    llm = ChatGroq(model=model, api_key=key, temperature=0.1)

    r = await llm.ainvoke([{"role": "user", "content": "Reply with exactly: GROQ_OK"}])
    print(f"Reply : {r.content.strip()}")
    print("\n✓  Groq is working! You can now start the server.")

asyncio.run(main())
