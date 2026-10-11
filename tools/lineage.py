"""
Generate a Mermaid data lineage diagram from dbt manifest.json.
Shows model dependencies and grain (from unique_combination tests).

Usage:
    python -m tools.lineage                      # prints to stdout
    python -m tools.lineage --output lineage.md  # writes to file
"""

import argparse
import json
import os
from pathlib import Path

MANIFEST_PATH = Path(__file__).parent.parent / "transforms" / "target" / "manifest.json"

SCHEMA_COLORS = {
    "bronze": "#cd7f32",
    "silver": "#c0c0c0",
    "gold":   "#ffd700",
}


def load_manifest(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def extract_models(manifest: dict) -> dict:
    return {
        key: node
        for key, node in manifest["nodes"].items()
        if node["resource_type"] == "model"
    }


def extract_grain(manifest: dict) -> dict[str, list[str]]:
    grain = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test":
            continue
        meta = node.get("test_metadata", {})
        if meta.get("name") != "unique_combination":
            continue
        columns = meta.get("kwargs", {}).get("columns", [])
        model_name = node["attached_node"].split(".")[-1]
        grain[model_name] = columns
    return grain


def build_plantuml(models: dict, grain: dict[str, list[str]]) -> str:
    lines = ["@startuml", "left to right direction", "skinparam defaultTextAlignment center", ""]

    # group nodes by schema
    by_schema: dict[str, list] = {}
    for node in models.values():
        by_schema.setdefault(node["config"]["schema"], []).append(node)

    for schema, nodes in by_schema.items():
        color = SCHEMA_COLORS.get(schema, "#eeeeee")
        lines.append(f'package "{schema}" {color} {{')
        for node in nodes:
            name = node["name"]
            g = grain.get(name)
            grain_str = f"\\n<size:10>grain: {', '.join(g)}</size>" if g else ""
            lines.append(f'    database "{name}{grain_str}" as {name}')
        lines.append("}")
        lines.append("")

    # edges
    for node in models.values():
        name = node["name"]
        deps = [d.split(".")[-1] for d in node["depends_on"]["nodes"] if "model" in d]
        for dep in deps:
            lines.append(f"{dep} --> {name}")

    lines.append("")
    lines.append("@enduml")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--output", default=None, help="output file path (default: stdout)")
    args = parser.parse_args()

    manifest = load_manifest(Path(args.manifest))
    models = extract_models(manifest)
    grain = extract_grain(manifest)
    diagram = build_plantuml(models, grain)

    out_path = Path(args.output) if args.output else Path(__file__).parent / "lineage.puml"
    out_path.write_text(diagram)
    print(f"Written to {out_path}")


if __name__ == "__main__":
    main()
