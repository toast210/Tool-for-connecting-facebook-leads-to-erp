"""Field mapping: turn a Graph lead (or website submission) into the flat ERPNext payload."""
import json
import re

from .config import (AD_FIELDS, DEFAULT_FIELD_MAPPING, DEFAULT_META_NAMES, PRIMARY_META, WEBSITE_META,
                     source_info)


def norm_key(s):
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")


def pretty_name(key):
    words = re.sub(r"[_\s]+", " ", str(key)).strip().split()
    return " ".join(w[:1].upper() + w[1:] for w in words) or str(key)


def clean_phone(v):
    v = v.strip()
    if v.lower().startswith("p:"):            # Meta sometimes prefixes numbers with "p:"
        v = v[2:]
    cleaned = re.sub(r"[^\d+]", "", v)
    return cleaned if re.search(r"\d", cleaned) else v


def payload_settings(cfg, website=False):
    """Mapping and options for building a payload. Website leads also get website.field_mapping
    (applied last, so it wins), which never affects Facebook leads."""
    p = cfg.get("payload") or {}
    mapping = {k: list(v) for k, v in DEFAULT_FIELD_MAPPING.items()}
    layers = [p.get("field_mapping") or {}]
    if website:
        layers.append((cfg.get("website") or {}).get("field_mapping") or {})
    for layer in layers:
        for out_name, src in layer.items():
            mapping[out_name] = [src] if isinstance(src, str) else list(src)
    names = dict(DEFAULT_META_NAMES)
    names.update(p.get("meta_field_names") or {})
    return {
        "mapping": mapping,
        "meta_names": names,
        "send_meta": p.get("send_meta_fields", list(DEFAULT_META_NAMES)),
        "include_unmapped": p.get("include_unmapped_answers", True),
        "drop": {norm_key(k) for k in p.get("drop_fields", [])},
        "required": p.get("required_fields", []),
        "phone_fields": set(p.get("phone_fields", ["Phone"])),
        "email_fields": set(p.get("email_fields", ["Email"])),
        "normalize": p.get("normalize_values", True),
        "static": p.get("static_fields") or {},
    }


def build_payload(row, graph, cfg):
    """Turn a Graph lead into the flat JSON sent to ERPNext (field matching happens here)."""
    origin = source_info(cfg, row["page_id"])
    ps = payload_settings(cfg, website=origin["website"])
    wh = json.loads(row["webhook_data"] or "{}")
    id_field = cfg["erpnext"].get("leadgen_id_field", "leadgen_id")
    if origin["website"]:        # no ads behind a website form: leave the ad/campaign fields out
        ps["send_meta"] = [k for k in ps["send_meta"] if k in WEBSITE_META]

    answers = {}        # normalised key -> (original key, value)
    for f in graph.get("field_data") or []:
        name = f.get("name")
        if name:
            answers[norm_key(name)] = (name, ", ".join(str(v) for v in (f.get("values") or [])).strip())

    used, out = set(), {}
    for out_name, sources in ps["mapping"].items():
        val = ""
        for src in sources:
            k = norm_key(src)
            if k in answers:
                used.add(k)
                if not val:
                    val = answers[k][1]
        out[out_name] = val
    if "Name" in out and not out["Name"]:       # build a name from first/last name when there is no full name
        first = answers.get("first_name", ("", ""))[1]
        last = answers.get("last_name", ("", ""))[1]
        if first or last:
            out["Name"] = f"{first} {last}".strip()
            used.update(k for k in ("first_name", "last_name") if k in answers)

    if ps["normalize"]:
        for k, v in out.items():
            if k in ps["phone_fields"] and v:
                out[k] = clean_phone(v)
            elif k in ps["email_fields"] and v:
                out[k] = v.strip().lower()
            elif k == "Name" and v:
                out[k] = " ".join(v.split())

    # ad / page data
    vals = {k: graph.get(k) for k in AD_FIELDS}
    for k, fb in (("ad_id", wh.get("ad_id")), ("adset_id", wh.get("adgroup_id")),
                  ("form_id", wh.get("form_id")), ("created_time", wh.get("created_time"))):
        if vals.get(k) is None:
            vals[k] = fb
    vals["page_id"] = str(row["page_id"])
    vals["lead_source"] = origin["source"]
    meta = {}
    for k in ps["send_meta"]:
        v = vals.get(k)
        meta[k] = ("" if v is None else v)

    def put_meta(keys):
        for k in keys:
            if k in meta:
                out[ps["meta_names"][k]] = meta[k]
    put_meta(PRIMARY_META)

    reserved = set(out) | {ps["meta_names"][k] for k in meta} | {id_field}
    if ps["include_unmapped"]:
        for nk, (orig, val) in answers.items():
            if nk in used or nk in ps["drop"] or not val:
                continue
            name = pretty_name(orig)
            if name in reserved:
                name += " (form)"
            out[name] = val
            reserved.add(name)
    for d in graph.get("custom_disclaimer_responses") or []:
        key = d.get("checkbox_key")
        if key:
            out[f"Disclaimer {pretty_name(key)}"] = d.get("status", d.get("value", ""))

    put_meta([k for k in meta if k not in PRIMARY_META])
    out.update(ps["static"])
    out[id_field] = row["leadgen_id"]
    return out
