# src/utils/token_accounting.py
"""
Token usage tracking and accounting.
Logs and reports token consumption from LLM API calls.
Variables and comments in English as per project guidelines.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class TokenAccounting:
    def __init__(self, logdir: Path = Path("data/token_logs"), session_id: Optional[str] = None):
        self.logdir = Path(logdir)
        self.logdir.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.sessionlogfile = self.logdir / f"tokens_{self.session_id}.jsonl"

    def log_tokens(self, query_tokens: int, context_tokens: int = 0, llm_tokens: int = 0, 
                   operation: str = "search", session_id: Optional[str] = None):
        # Align session_id if provided from Streamlit
        if session_id and session_id != self.session_id:
            old_file = self.sessionlogfile
            self.session_id = session_id
            self.sessionlogfile = self.logdir / f"tokens_{self.session_id}.jsonl"
            logger.info(f"TokenAccounting aligned to session_id: {self.session_id} (from {old_file.name})")
        
        total = query_tokens + context_tokens + llm_tokens
        record = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "query_tokens": query_tokens,
            "context_tokens": context_tokens,
            "llm_tokens": llm_tokens,
            "total_tokens": total
        }
        self._write_record(record)
        logger.info(f"{operation}: query={query_tokens}, context={context_tokens}, llm={llm_tokens}, total={total}")

    def _write_record(self, record: Dict):
        try:
            with open(self.sessionlogfile, 'a', encoding='utf-8') as f:
                f.write(json.dumps(record) + '\n')
        except Exception as e:
            logger.error(f"Error writing token log: {e}")

    def load_session_logs(self) -> List[Dict]:
        logs = []
        try:
            if self.sessionlogfile.exists():
                with open(self.sessionlogfile, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            logs.append(json.loads(line))
        except Exception as e:
            logger.error(f"Error loading token logs: {e}")
        return logs

    def get_session_report(self) -> Dict:
        logs = self.load_session_logs()
        vectorizations = [l for l in logs if l["operation"] == "vectorization"]
        searches = [l for l in logs if l["operation"] in ["search", "chat"]]
        
        report = {
            "session_id": self.session_id,
            "session_start": logs[0]["timestamp"] if logs else None,
            "session_end": logs[-1]["timestamp"] if logs else None,
            "vectorization": {
                "operations": len(vectorizations),
                "total_chunks": sum(l.get("chunks", 0) for l in vectorizations),
                "total_tokens": sum(l.get("tokens", 0) for l in vectorizations),
                "avg_tokens_per_chunk": sum(l.get("tokens", 0) for l in vectorizations) / 
                                        sum(l.get("chunks", 0) for l in vectorizations) if 
                                        any(l.get("chunks", 0) > 0 for l in vectorizations) else 0
            },
            "searches": {
                "operations": len(searches),
                "total_tokens": sum(l.get("total_tokens", 0) for l in searches),
                "avg_tokens_per_search": sum(l.get("total_tokens", 0) for l in searches) / len(searches) if searches else 0
            }
        }
        report["total_tokens"] = report["vectorization"]["total_tokens"] + report["searches"]["total_tokens"]
        return report

# Global instance (must be defined before functions)
accounting: Optional[TokenAccounting] = None

def get_accounting(session_id: Optional[str] = None, logdir: Optional[Path] = None) -> TokenAccounting:
    global accounting
    if accounting is None:
        accounting = TokenAccounting(logdir=logdir or Path("data/token_logs"), session_id=session_id)
    elif session_id and session_id != accounting.session_id:
        accounting = TokenAccounting(logdir=accounting.logdir, session_id=session_id)
    return accounting

def reset_accounting():
    global accounting
    accounting = None

