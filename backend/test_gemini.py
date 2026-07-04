"""Quick smoke test — run: python test_gemini.py"""
import os, asyncio
from dotenv import load_dotenv
load_dotenv()

from langchain_google_genai import ChatGoogleGenerativeAI

async def main():
    key   = os.environ.get("GOOGLE_API_KEY","")
    model = os.getenv("LLM_MODEL", "gemini-2.0-flash")
    print(f"Key  : {key[:8]}...{key[-4:]}")
    print(f"Model: {model}")

    llm = ChatGoogleGenerativeAI(
        model=model,
        google_api_key=key,
        convert_system_message_to_human=True,
    )
    r = await llm.ainvoke([{"role": "user", "content": "Say HELLO in one word."}])
    print(f"Reply: {r.content.strip()}")
    print("✓ Gemini is working correctly!")

asyncio.run(main())
