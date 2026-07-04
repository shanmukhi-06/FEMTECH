"""
Loads a past crime record for Marcus Harlow into a SEPARATE Cognee dataset
to simulate a historical case file.

This tests cross-case memory retrieval — the current interrogation agent
should be able to recall facts from a completely different case file.

Run: python load_past_crime.py
"""
import asyncio, os
from dotenv import load_dotenv
load_dotenv()

PAST_CASE_ID = "case-2021-harlow-fraud"

PAST_CRIME_RECORDS = [
    # Case background
    "[CASE-2021] Subject: Marcus Harlow, DOB 1981-04-22. "
    "Case opened: November 3, 2021. Charge: Corporate fraud and embezzlement "
    "at DataVault Inc, previous employer. Amount: $247,000 diverted over 8 months.",

    # Harlow's statement in 2021
    "[CASE-2021] [stmt_001] Harlow stated: I have never manipulated financial "
    "records at DataVault. All transactions I processed were approved by my manager "
    "Richard Chen. I was a victim of a setup by the finance department.",

    # Witness statement
    "[CASE-2021] Witness Richard Chen stated: I never approved any transactions "
    "above $50,000 without a paper trail. Harlow had exclusive access to the "
    "sub-ledger system between March and October 2021.",

    # Evidence
    "[CASE-2021] Digital forensics confirmed: Harlow logged into the sub-ledger "
    "system from his home IP address on 14 occasions after business hours. "
    "He denied ever accessing the system remotely.",

    # Contradiction in 2021 case
    "[CASE-2021] CONTRADICTION flagged: Harlow claimed he never accessed systems "
    "remotely, but server logs show 14 remote logins from IP 192.168.1.45 "
    "registered to 14 Birchwood Drive, San Francisco — Harlow's home address.",

    # Outcome
    "[CASE-2021] Case outcome: Charges dropped due to insufficient direct evidence. "
    "Harlow resigned from DataVault. Civil settlement of $180,000 paid. "
    "Harlow denies any wrongdoing. Flagged for future monitoring.",

    # MO pattern
    "[CASE-2021] Investigator note: Subject displays a consistent pattern of "
    "initial denial followed by gradual admission when confronted with evidence. "
    "Subject claims memory lapses for incriminating events. High deception risk profile.",
]


async def main():
    import cognee

    os.environ["LLM_PROVIDER"] = "openai"
    os.environ["LLM_MODEL"]    = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
    os.environ["LLM_API_KEY"]  = os.environ["GROQ_API_KEY"]
    os.environ["LLM_ENDPOINT"] = "https://api.groq.com/openai/v1"
    os.environ.setdefault("EMBEDDING_PROVIDER", "local")

    print(f"Loading past crime file into dataset: {PAST_CASE_ID}")
    print("-" * 60)

    for i, record in enumerate(PAST_CRIME_RECORDS):
        print(f"  [{i+1}/{len(PAST_CRIME_RECORDS)}] Storing: {record[:70]}…")
        await cognee.remember(
            record,
            dataset_name=PAST_CASE_ID,
            run_in_background=False,
            self_improvement=False,
        )

    print("\n✓ Past crime file loaded successfully!")
    print(f"  Dataset: {PAST_CASE_ID}")
    print(f"  Records: {len(PAST_CRIME_RECORDS)}")

    # Verify with a recall
    print("\nVerifying with recall()...")
    results = await cognee.recall(
        query_text="What did Harlow say about remote access and financial records?",
        datasets=[PAST_CASE_ID],
        top_k=3,
    )
    print(f"  Recall returned {len(results)} results")
    for r in results[:2]:
        text = getattr(r, "text", None) or getattr(r, "answer", str(r))
        print(f"  → {str(text)[:150]}")

    print("\n✓ Cross-case file is ready to query from the main interrogation!")


asyncio.run(main())
