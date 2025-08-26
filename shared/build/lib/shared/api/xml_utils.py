from __future__ import annotations

from typing import Any, Type

from marshmallow import Schema, fields


def xml_items_to_dict_extractor(items_xpath: str, field_map: dict):
    """Return an extractor that maps item XML nodes to dictionaries.

    field_map can contain:
      - key: xpath string (string() is applied)
      - key: {"xpath": str, "multi": bool=False, "join": str|None}
    """

    def _extract(payload):
        try:
            items = payload.xpath(items_xpath)  # type: ignore[attr-defined]
        except Exception:
            return []

        rows = []
        for item in items:
            row = {}
            for key, spec in field_map.items():
                try:
                    if isinstance(spec, str):
                        val = item.xpath(f"string({spec})")  # type: ignore[attr-defined]
                    else:
                        xpath_expr = spec.get("xpath", "")
                        if spec.get("multi"):
                            nodes = item.xpath(xpath_expr)  # type: ignore[attr-defined]
                            texts = [(n.text or "") for n in nodes]
                            joiner = spec.get("join")
                            val = (
                                joiner.join([t for t in texts if t])
                                if joiner
                                else texts
                            )
                        else:
                            val = item.xpath(f"string({xpath_expr})")  # type: ignore[attr-defined]
                except Exception:
                    val = ""
                row[key] = val if val is not None else ""
            rows.append(row)
        return rows

    return _extract


def extractor_from_schema(items_xpath: str, schema_cls: Type[Schema]):
    """Build an extractor from a Marshmallow schema with XPath metadata.

    - items_xpath: selects row nodes
    - schema field.metadata["xpath"] for scalar fields
    - For joined string lists, use field.metadata with keys:
      {"container_xpath", "item_xpath", "multi": True, "join": ","}
    """

    def _extract(root):
        try:
            nodes = root.xpath(items_xpath)  # type: ignore[attr-defined]
        except Exception:
            return []
        return [_extract_object(node, schema_cls) for node in nodes]

    return _extract


def _extract_object(node, schema_cls: Type[Schema]):
    result: dict[str, Any] = {}
    fields_map = getattr(schema_cls, "_declared_fields", {})
    for field_name, field in fields_map.items():
        meta = getattr(field, "metadata", {}) or {}

        # Handle joined list of strings into single STRING field (using metadata)
        if meta.get("multi") and meta.get("item_xpath"):
            container_xpath = (meta.get("container_xpath") or "").strip()
            item_xpath = (meta.get("item_xpath") or "").strip()
            parent = node
            if container_xpath:
                try:
                    parents = node.xpath(container_xpath)  # type: ignore[attr-defined]
                    parent = parents[0] if parents else node
                except Exception:
                    parent = node
            try:
                items = parent.xpath(item_xpath)  # type: ignore[attr-defined]
            except Exception:
                items = []
            texts: list[str] = []
            for it in items:
                try:
                    texts.append(
                        str(it)
                        if isinstance(it, str)
                        else (getattr(it, "text", "") or "")
                    )
                except Exception:
                    texts.append("")
            joiner = meta.get("join")
            value = joiner.join([t for t in texts if t]) if joiner else texts
            result[field_name] = value
            continue

        # Scalar via xpath
        xp = meta.get("xpath")
        if xp:
            try:
                value = node.xpath(f"string({xp})")  # type: ignore[attr-defined]
            except Exception:
                value = ""
            result[field_name] = value

    return result


__all__ = [
    "xml_items_to_dict_extractor",
    "extractor_from_schema",
]
