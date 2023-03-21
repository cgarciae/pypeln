import inspect
import shutil
from pathlib import Path

import pypeln as pl

shutil.rmtree("docs/api")

docs = """
# pl.{module}.{f}

::: pypeln.{module}.{f}
"""

module_docs = """
# pl.{module}

::: pypeln.{module}
    selection:
      members:
        - na


### Members
{members}
"""


structure = ""
for module in ["process", "thread", "task", "sync"]:
    structure += f"      - {module}:\n"
    structure += f"          Overview: api/{module}/Overview.md\n"
    members = ""

    for f in sorted(
        [
            f.__name__
            for f in vars(pl.process).values()
            if inspect.isfunction(f) and "api" in inspect.getmodule(f).__name__
        ]
    ):
        structure += f"          {f}: api/{module}/{f}.md\n"
        members += "* [{f}]({f}.md)\n".format(f=f)

        path = Path(f"docs/api/{module}/{f}.md")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(docs.format(module=module, f=f))

    Path(f"docs/api/{module}/Overview.md").write_text(
        module_docs.format(module=module, members=members)
    )


mkdocs = Path("scripts/mkdocs_template.yml").read_text()
mkdocs = mkdocs.format(structure=structure)
Path("mkdocs.yml").write_text(mkdocs)
# %%
