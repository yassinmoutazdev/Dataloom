# Documentation Standards

> **For AI Coding Assistants:** These rules are active for every file in this codebase.
> When writing, editing, or reviewing any code, enforce these standards automatically.
> Do not ask for permission to follow them — just apply them.

---

## Philosophy

**Code explains WHAT. Documentation explains WHY.**

Three rules govern everything here:

1. **No redundancy.** Do not restate what the code already makes obvious.
2. **Minimum viable clarity.** Use the fewest words that fully communicate intent.
3. **Write for the next person.** Your audience is a competent developer reading this in six months with no context.

---

## Documentation Types

### 1. Inline Comments

Use inline comments to explain **intent, trade-offs, and non-obvious decisions** — not mechanics.

**When to write one:**
- The "why" behind a choice would not be obvious from reading the code
- A workaround, hack, or constraint exists that future developers must know about
- An edge case is being handled in a way that looks wrong but is correct

**When NOT to write one:**
- The code is self-explanatory
- You are just narrating what the next line does
- The variable or function name already communicates the intent

```python
# GOOD — explains a non-obvious constraint
# Offset by 1 because the API returns 1-indexed page numbers
page_index = response["page"] - 1

# BAD — restates what the code already shows
# Add 1 to counter
counter += 1
```

```python
# GOOD — flags a deliberate trade-off
# Using linear scan here; dataset is always < 50 items so O(n) is acceptable
match = next((x for x in items if x["id"] == target_id), None)

# BAD — explains Python syntax to a Python developer
# Use next() to get the first item from the generator
match = next((x for x in items if x["id"] == target_id), None)
```

**Format rules:**
- Place the comment on the line **above** the code it describes, not inline at the end (unless it is a very short clarification)
- Write in full sentences with correct grammar
- Keep to one or two lines maximum; if you need more, consider a docstring instead

---

### 2. Docstrings / Documentation Comments

Every **public function, class, and module** must have a docstring. Private helpers only need one when their logic is non-trivial.

A docstring must answer:
- What does this do? (one-line summary)
- What does it accept? (parameters, types, constraints)
- What does it return or raise?
- Any important side effects or caveats?

**Default style: Google format** (unless the project specifies otherwise — see Language Conventions below).

```python
# GOOD — complete, scannable, no padding
def calculate_discount(price: float, tier: str) -> float:
    """Apply a tier-based discount to a base price.

    Args:
        price: The original price in USD. Must be >= 0.
        tier: Customer tier. One of 'standard', 'premium', 'enterprise'.

    Returns:
        The discounted price. Returns the original price if tier is unrecognized.

    Raises:
        ValueError: If price is negative.
    """
```

```python
# BAD — vague, no parameter types, adds no information
def calculate_discount(price, tier):
    """Calculates the discount."""
```

**Docstring rules:**
- The first line is a single, imperative-mood sentence (e.g., "Apply a discount", not "This function applies a discount")
- Leave a blank line between the summary and the Args/Returns sections
- Document all parameters, including their types and any constraints (valid ranges, allowed values)
- Document all exceptions the caller must handle
- Do not document internal implementation steps — that belongs in inline comments inside the function body

---

### 3. Module-Level Documentation

Every file must begin with a module docstring that describes:
- The **purpose** of the module (one to two sentences)
- What it **owns** in the architecture (which domain or layer)
- Any **important dependencies or assumptions** a developer needs to know before editing

```python
"""
User authentication and session management.

Handles token issuance, validation, and refresh flows. Relies on the Redis
session store configured in config/cache.py. All functions assume the request
context is already authenticated at the route level.
"""
```

Keep module docstrings under five lines. If you need more, the module is probably doing too much.

---

## Audience Calibration

Adjust documentation depth based on who will read it.

| Context | Depth | Notes |
|---|---|---|
| Internal codebase | Lean | Assume competent developers. Skip basics. |
| Public-facing API / SDK | Thorough | Explain parameters, provide usage examples, document errors |
| Onboarding-critical code | Annotated | Add extra context where architecture decisions are non-obvious |
| Throwaway / prototype | Minimal | A module docstring is enough |

Do not reference internal tickets, jargon, or private systems in any documentation that will be public-facing.

---

## Language-Specific Conventions

| Language | Docstring Format | Auto-Doc Tool | Standard Reference |
|---|---|---|---|
| Python | Google style (default), NumPy, or reStructuredText | Sphinx | PEP 257 |
| JavaScript / TypeScript | JSDoc | JSDoc CLI, TypeDoc | JSDoc standard |
| Java | Javadoc | `javadoc` tool | Oracle Javadoc guide |
| Ruby | YARD | YARD | YARD getting started |
| Go | GoDoc (plain comments above declarations) | `go doc` | Effective Go |
| C# | XML doc comments | `dotnet doc` | Microsoft docs |

When contributing to a codebase in a language other than Python, use the format from this table. If unsure, ask the AI assistant to generate the correct format and verify against the reference.

---

## What to Keep Up to Date

Documentation rot causes more confusion than no documentation at all.

**Update the docstring or comment whenever you:**
- Change a function's signature, parameters, or return type
- Change the behavior of an existing function
- Add or remove a raised exception
- Refactor logic in a way that changes intent, not just structure
- Deprecate something

**Stale documentation is a bug.** If you notice a comment or docstring that no longer matches the code, fix it in the same PR — do not leave it for later.

---

## AI Enforcement Rules

> These rules are directives for AI coding assistants operating in this codebase.

**When generating new code:**
- Always include a docstring for every new public function, class, and module
- Use Google docstring format for Python unless otherwise specified in this file
- Write inline comments only where the reasoning is non-obvious
- Do not add comments that restate what the code already shows
- Match the documentation depth to the audience context established above

**When editing existing code:**
- If you change a function's behavior, update its docstring in the same edit
- If you find a docstring that contradicts the current code, flag it and correct it
- Do not remove existing comments unless they are factually wrong or genuinely redundant

**When reviewing or auditing documentation:**
- Flag any docstring missing required sections (Args, Returns, Raises where applicable)
- Flag any inline comment that only restates the code
- Flag any module missing a module-level docstring
- Suggest concise rewrites for any comment that is verbose without adding information

**Format consistency:**
- Docstrings use the project's chosen style (default: Google)
- Do not mix styles within a single file
- Follow PEP 257 formatting rules for Python (triple double-quotes, blank line after summary, consistent indentation)

---

## Quick Reference: Good vs. Bad

```python
# --- INLINE COMMENTS ---

# GOOD
# Retry limit matches the SLA window in the vendor contract (3 attempts, 30s each)
MAX_RETRIES = 3

# BAD
# Set max retries to 3
MAX_RETRIES = 3


# --- DOCSTRINGS ---

# GOOD
def parse_webhook_payload(raw: bytes, secret: str) -> dict:
    """Verify and decode an incoming webhook payload.

    Validates the HMAC signature before parsing. Raises immediately
    if the signature is invalid to prevent processing tampered data.

    Args:
        raw: The raw request body bytes.
        secret: The shared HMAC secret for this webhook source.

    Returns:
        Parsed payload as a dictionary.

    Raises:
        SignatureError: If the HMAC signature does not match.
        ValueError: If the payload is not valid JSON.
    """

# BAD
def parse_webhook_payload(raw, secret):
    """Parse the webhook."""


# --- MODULE DOCSTRING ---

# GOOD
"""
Stripe webhook event processing.

Receives, validates, and routes inbound Stripe events to the appropriate
domain handlers. Depends on the WebhookRouter defined in routing/stripe.py.
"""

# BAD
"""This module handles webhooks."""
```

---

## Canonical File Locations

| What | Where |
|---|---|
| This standards file | `docs/DOCUMENTATION_STANDARDS.md` or project root |
| Sphinx config | `docs/conf.py` |
| API reference source | `docs/api/` |
| Changelog | `CHANGELOG.md` in project root |

---

*This file is the single source of truth for documentation conventions in this codebase. When in doubt, check here first.*
