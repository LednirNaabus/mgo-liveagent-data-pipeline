from typing import List, Dict, Any

def extract_role_text_dict(
    messages: List[Dict[str, Any]],
    *,
    include_metadata: bool = False,
    collapse_bot_chunks: bool = False
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def _meta(m: Dict[str, Any]) -> Dict[str, Any]:
        if not include_metadata:
            return {}
        return {
            "datetime": m.get("datetime"),
            "message_id": m.get("message_id"),
            "type": m.get("type")
        }

    for m in messages:
        role = m.get("role", "Unknown")
        content = m.get("content", {})

        if isinstance(content, dict) and content.get("type") == "text":
            text = content.get("text")
            if text:
                item = {"role": role, "content": text}
                item.update(_meta(m))
                out.append(item)
            continue