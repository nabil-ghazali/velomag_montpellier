## Purpose
Provide concise, actionable instructions for AI coding agents working on this repo (backend).

**Big Picture**
- **Service**: a minimal Python backend with a small `app/` package and a runnable `main.py` at repository root.
- **Data source**: the Jupyter notebook `notebook_backend.ipynb` shows live integration with the Montpellier open API at `https://portail-api-data.montpellier3m.fr` (see the `ecocounter` endpoint). Agents should treat that external API as the primary integration point.
- **Why**: this repo appears to be a lightweight data-fetching/processing backend (not yet a web service). Changes should avoid assuming frameworks (FastAPI/Flask) unless added explicitly.

**How to run locally**
- Run the simple script: `python main.py` (root `main.py` prints a short message).
- The project uses `pyproject.toml` for metadata and declares `requires-python = ">=3.12"`. Prefer using a Python 3.12+ environment.
- Dependencies are declared in `pyproject.toml` (`ipykernel`, `requests`). `requirements.txt` is currently empty — prefer `pyproject.toml` as the source of truth.

**Key files and what they mean**
- `main.py` (root): lightweight entrypoint used for quick local checks.
- `app/` directory: intended package; currently `app/main.py` is empty — treat it as the place to add application logic if expanding the backend.
- `notebook_backend.ipynb`: exploratory notebook showing how the team fetches ecocounter lists. Important details are discoverable here (example: `params = {"limit": 1000}` — the API requires a `limit` param in requests).
- `pyproject.toml`: canonical dependency and Python version info.
- `Dockerfile`: present but empty — don't assume an existing container workflow.

**Patterns and conventions to follow**
- Use `pyproject.toml` as the authoritative dependency file. If adding a `requirements.txt`, keep it in sync.
- Keep `app/` package imports relative and package-first (package created under `app`), do not change project layout without a PR and brief justification.
- Do not introduce a web framework unless there's a clear change in project scope; if you do, update `README.md`, `pyproject.toml`, and add a simple run instruction.

**Integration specifics**
- External API base: `https://portail-api-data.montpellier3m.fr`. The notebook demonstrates the required query parameters and the two-step flow: (1) list ecocounters (`/ecocounter`), (2) fetch data per counter ID.
- Network calls in notebooks use `requests`. Keep `requests` for simple scripts; if adding async support, document the reason and update dependencies.

**What an agent should do when asked to modify code**
- Read `notebook_backend.ipynb` for the intended data flow before changing fetching logic.
- If you add new files, update `pyproject.toml` and `README.md` to document the change.
- Add minimal tests when implementing non-trivial logic and include a short run example in `README.md`.

**Examples (copyable snippets from repo)**
- Fetch ecocounters (from `notebook_backend.ipynb`):
```
BASE_URL = "https://portail-api-data.montpellier3m.fr"
url_list = f"{BASE_URL}/ecocounter"
params_list = {"limit": 1000}
r_list = requests.get(url_list, params=params_list)
```

**CI / Tests / Docker**
- No CI or tests are present. If you add CI, use `pyproject.toml` to install deps and run tests with `pytest`.
- `Dockerfile` is empty — do not create assumptions about container runtime.

**PR guidance for agents**
- Small, focused commits. Update `pyproject.toml` for dependency changes. Document behavioral or API changes in `README.md` and the top of the changed module.

If anything above is unclear or you want more detail about a specific area (e.g., intended API shape, expected output formats, or preferred test layout), say which area and I'll expand this file.
