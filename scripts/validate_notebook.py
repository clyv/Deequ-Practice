# -*- coding: utf-8 -*-
"""Static validation for the pipeline notebook.

Notebooks fail late and expensively: a name that is never defined surfaces only
when a cell runs, which here meant a ten-minute Spark install first. This walks
the cells in execution order and reports names read before anything binds them.

It would have caught the original defect in this repo, where the metrics-store
cells referenced AnalysisRunner, ResultKey, `repository` and `metrics_df` with
nothing defining them -- 13 undefined reads, NameError on the first run.

Usage:  python scripts/validate_notebook.py notebooks/project1.ipynb
"""
from __future__ import annotations

import ast
import builtins
import json
import re
import sys
from pathlib import Path

# IPython magics are not Python; neutralise them before parsing.
MAGIC_ASSIGN = re.compile(r"^(\s*)([A-Za-z_]\w*)\s*=\s*!.*$")
MAGIC = re.compile(r"^(\s*)[!%].*$")

# Names the notebook runtime provides.
NOTEBOOK_BUILTINS = {"get_ipython", "display", "In", "Out", "__name__", "__file__"}


def desugar(source: str) -> str:
    out = []
    for line in source.split("\n"):
        assigned = MAGIC_ASSIGN.match(line)
        if assigned:
            out.append(f"{assigned.group(1)}{assigned.group(2)} = []")
            continue
        magic = MAGIC.match(line)
        out.append(f"{magic.group(1)}pass" if magic else line)
    return "\n".join(out)


def bound_names(tree: ast.AST) -> set[str]:
    """Every name this tree binds, generously interpreted."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            names.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
            args = getattr(node, "args", None)
            if args:
                names.update(a.arg for a in args.args + args.kwonlyargs + args.posonlyargs)
                if args.vararg:
                    names.add(args.vararg.arg)
                if args.kwarg:
                    names.add(args.kwarg.arg)
        elif isinstance(node, ast.Lambda):
            a = node.args
            names.update(x.arg for x in a.args + a.kwonlyargs + a.posonlyargs)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            names.update(node.names)
    return names


def validate(path: Path) -> list[str]:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    defined = set(dir(builtins)) | NOTEBOOK_BUILTINS
    problems: list[str] = []

    for index, cell in enumerate(notebook["cells"]):
        if cell.get("cell_type") != "code":
            continue
        code = desugar("".join(cell["source"]))
        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            problems.append(f"cell {index}: syntax error: {exc}")
            continue

        used = {
            n.id for n in ast.walk(tree)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
        }
        binds = bound_names(tree)
        problems += [
            f"cell {index}: reads undefined name {name!r}"
            for name in sorted(used - binds - defined)
        ]
        defined |= binds

    return problems


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    path = Path(sys.argv[1])
    problems = validate(path)
    if problems:
        print(f"FAIL {path}")
        for problem in problems:
            print(f"  {problem}")
        return 1
    count = len(json.loads(path.read_text(encoding="utf-8"))["cells"])
    print(f"OK {path}: {count} cells, every name defined before it is read.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
