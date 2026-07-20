"""Presentation helpers for the Streamlit UI.

Pure styling/layout only — no business logic. Provides the app-wide CSS, a
horizontal stepper for the fast workflows, and small layout wrappers so results
screens stay consistent. Icons use Google Material Symbols via Streamlit's
`:material/<name>:` directive and the `icon=` argument on widgets — never emoji.
"""

from __future__ import annotations

from typing import List

import streamlit as st


_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,400,0,0');

.rc-ic {
  font-family: 'Material Symbols Rounded';
  font-weight: normal;
  font-style: normal;
  line-height: 1;
  letter-spacing: normal;
  text-transform: none;
  display: inline-block;
  white-space: nowrap;
  word-wrap: normal;
  direction: ltr;
  font-feature-settings: 'liga';
  -webkit-font-smoothing: antialiased;
  vertical-align: -0.15em;
}

:root {
  --rc-primary: #2563EB;
  --rc-primary-dark: #1D4ED8;
  --rc-primary-soft: #E7EEFE;
  --rc-ink: #12203A;
  --rc-body: #334155;
  --rc-muted: #5B6B7F;
  --rc-border: #E2E8F0;
  --rc-border-strong: #CBD5E1;
  --rc-bg: #F7F9FC;
  --rc-surface: #FFFFFF;
  --rc-bg-soft: #EEF2F7;
  --rc-success: #047857;
  --rc-warning: #B45309;
  --rc-danger: #B91C1C;
  --rc-shadow: 0 1px 2px rgba(15,23,42,.04), 0 2px 8px rgba(15,23,42,.05);
  --rc-shadow-lift: 0 6px 20px rgba(37,99,235,.18);
  --rc-ease: cubic-bezier(0.22, 1, 0.36, 1);
}

/* ---- Base ---- */
.stApp { background: var(--rc-bg); }
.block-container { padding-top: 2.4rem; padding-bottom: 4rem; max-width: 1140px; }

html, body, [class*="css"] {
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}

/* Headings: fixed scale, balanced wrapping, tight tracking. */
h1, h2, h3, h4 {
  color: var(--rc-ink);
  font-weight: 700;
  letter-spacing: -0.01em;
  text-wrap: balance;
}
h1 { font-size: 1.9rem; }
h2 { font-size: 1.35rem; }
h3 { font-size: 1.15rem; }

/* Body copy: readable, not washed out. */
.stApp p, .stApp li, [data-testid="stMarkdownContainer"] p { color: var(--rc-body); }
.stCaption, [data-testid="stCaptionContainer"], small { color: var(--rc-muted) !important; }

/* Tabular figures everywhere numbers matter. */
[data-testid="stMetricValue"], [data-testid="stDataFrame"] { font-variant-numeric: tabular-nums; }

/* ---- Buttons ---- */
.stButton > button, .stDownloadButton > button {
  border-radius: 10px;
  font-weight: 600;
  border: 1px solid var(--rc-border-strong);
  color: var(--rc-ink);
  background: var(--rc-surface);
  transition: transform .12s var(--rc-ease), box-shadow .18s var(--rc-ease),
              background .18s var(--rc-ease), border-color .18s var(--rc-ease);
}
.stButton > button:hover, .stDownloadButton > button:hover {
  transform: translateY(-1px);
  border-color: var(--rc-primary);
  box-shadow: var(--rc-shadow-lift);
}
.stButton > button:active, .stDownloadButton > button:active { transform: translateY(0); box-shadow: none; }
.stButton > button:focus-visible, .stDownloadButton > button:focus-visible {
  outline: 3px solid var(--rc-primary-soft);
  outline-offset: 1px;
  border-color: var(--rc-primary);
}
.stButton > button[kind="primary"], .stDownloadButton > button[kind="primary"] {
  background: var(--rc-primary);
  border-color: var(--rc-primary);
  color: #fff;
}
/* Force the label + icon inside primary buttons to white. The label renders in a
   nested <p>/markdown container that otherwise inherits the dark body-text color;
   cover the button and every descendant, by kind and by Streamlit test-id. */
.stButton > button[kind="primary"], .stButton > button[kind="primary"] *,
.stDownloadButton > button[kind="primary"], .stDownloadButton > button[kind="primary"] *,
button[data-testid="stBaseButton-primary"], button[data-testid="stBaseButton-primary"] * {
  color: #fff !important;
}
.stButton > button[kind="primary"]:hover, .stDownloadButton > button[kind="primary"]:hover {
  background: var(--rc-primary-dark);
  border-color: var(--rc-primary-dark);
}
.stButton > button[disabled], .stButton > button[disabled]:hover {
  transform: none; box-shadow: none; opacity: .55;
}

/* ---- Metric tiles ---- */
div[data-testid="stMetric"] {
  background: var(--rc-surface);
  border: 1px solid var(--rc-border);
  border-radius: 14px;
  padding: 16px 18px;
  box-shadow: var(--rc-shadow);
}
div[data-testid="stMetricLabel"] p { color: var(--rc-muted) !important; font-weight: 600; }
div[data-testid="stMetricValue"] { color: var(--rc-ink); }

/* ---- File uploader ---- */
div[data-testid="stFileUploaderDropzone"] {
  border-radius: 14px;
  border: 1.5px dashed var(--rc-border-strong);
  background: #FBFCFE;
  transition: border-color .18s var(--rc-ease), background .18s var(--rc-ease);
}
div[data-testid="stFileUploaderDropzone"]:hover { border-color: var(--rc-primary); background: var(--rc-primary-soft); }

/* ---- Dataframe ---- */
div[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid var(--rc-border); }

/* ---- Sidebar ---- */
section[data-testid="stSidebar"] { background: var(--rc-surface); border-right: 1px solid var(--rc-border); }

/* Material icons sit on the text baseline nicely. */
[data-testid="stIconMaterial"] { vertical-align: -0.15em; }

/* ---- Home hero ---- */
.rc-hero { display: flex; align-items: center; gap: 18px; margin-bottom: 6px; }
.rc-hero-logo { width: 60px; height: 60px; flex-shrink: 0; }
.rc-hero-title { margin: 0; font-size: 1.75rem; font-weight: 700; color: var(--rc-ink); letter-spacing: -0.015em; }
.rc-hero-sub { color: var(--rc-body); font-size: 1.02rem; margin: 2px 0 0 0; }

/* ---- Home option cards (native bordered container + button inside) ---- */
/* Paired cards in a row stretch to equal height; the button pins to the bottom
   so the two buttons always line up regardless of description length. */
[data-testid="stHorizontalBlock"]:has(.rc-opt) { align-items: stretch; }
[data-testid="stVerticalBlockBorderWrapper"]:has(.rc-opt) {
  border-radius: 16px;
  border-color: var(--rc-border);
  box-shadow: var(--rc-shadow);
  height: 100%;
  transition: border-color .18s var(--rc-ease), box-shadow .18s var(--rc-ease);
}
[data-testid="stVerticalBlockBorderWrapper"]:has(.rc-opt):hover {
  border-color: var(--rc-border-strong);
  box-shadow: 0 6px 20px rgba(15,23,42,.08);
}
[data-testid="stVerticalBlockBorderWrapper"]:has(.rc-opt) > div { height: 100%; }
[data-testid="stVerticalBlockBorderWrapper"]:has(.rc-opt) [data-testid="stVerticalBlock"] {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
}
[data-testid="stVerticalBlockBorderWrapper"]:has(.rc-opt) [data-testid="stElementContainer"]:last-child {
  margin-top: auto;
}
.rc-opt-title { display: flex; align-items: center; gap: 9px; margin: 2px 0 6px 0; }
.rc-opt-title .rc-ic { color: var(--rc-primary); font-size: 1.4rem; }
.rc-opt-title span { font-size: 1.12rem; font-weight: 700; color: var(--rc-ink); letter-spacing: -0.01em; }
.rc-opt-desc { color: var(--rc-body); font-size: .92rem; line-height: 1.45; margin: 0; }

/* ---- Stepper ---- */
.rc-stepper { display: flex; align-items: center; gap: 0; margin: 8px 0 26px 0; }
.rc-step { display: flex; align-items: center; gap: 10px; flex: 0 1 auto; }
.rc-dot {
  width: 30px; height: 30px; border-radius: 50%; flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
  font-size: .85rem; font-weight: 700;
  transition: background .2s var(--rc-ease), color .2s var(--rc-ease), border-color .2s var(--rc-ease);
}
.rc-dot .rc-ic { font-size: 1.1rem; line-height: 1; }
.rc-dot-done { background: var(--rc-primary); color: #fff; }
.rc-dot-active { background: #fff; color: var(--rc-primary); border: 2px solid var(--rc-primary); box-shadow: 0 0 0 4px var(--rc-primary-soft); }
.rc-dot-todo { background: var(--rc-bg-soft); color: var(--rc-muted); border: 1px solid var(--rc-border-strong); }
.rc-step-label { font-size: .9rem; font-weight: 600; white-space: nowrap; }
.rc-step-label-active { color: var(--rc-ink); }
.rc-step-label-todo { color: var(--rc-muted); }
.rc-connector { height: 2px; flex: 1 1 24px; min-width: 16px; background: var(--rc-border); margin: 0 12px; border-radius: 2px; }
.rc-connector-done { background: var(--rc-primary); }

/* ---- Checklist (processing status) ---- */
.rc-check { display: flex; align-items: center; gap: 9px; padding: 3px 0; font-size: .93rem; color: var(--rc-body); }
.rc-check .rc-ic { font-size: 1.15rem; line-height: 1; }
.rc-check-done .rc-ic { color: var(--rc-success); }
.rc-check-active { color: var(--rc-ink); font-weight: 600; }
.rc-check-active .rc-ic { color: var(--rc-primary); }
.rc-check-todo { color: var(--rc-muted); }
.rc-check-todo .rc-ic { color: var(--rc-border-strong); }

@media (max-width: 640px) {
  .rc-step-label { display: none; }
  .rc-connector { margin: 0 6px; }
}

@media (prefers-reduced-motion: reduce) {
  * { transition: none !important; animation: none !important; }
  .stButton > button:hover, .stDownloadButton > button:hover { transform: none; }
}
</style>
"""

# Material Symbols glyph names used across the app (single source of truth).
IC = {
    "home": "home",
    "back": "arrow_back",
    "forward": "arrow_forward",
    "restart": "restart_alt",
    "delete": "delete",
    "run": "play_arrow",
    "download": "download",
    "zip": "folder_zip",
    "fast": "bolt",
    "one_file": "description",
    "many_files": "folder_open",
    "done": "check_circle",
    "active": "pending",
    "todo": "radio_button_unchecked",
    "clean": "cleaning_services",
    "filter": "filter_alt",
    "complete": "celebration",
    "results": "insights",
    "table": "table_rows",
    "upload": "upload_file",
    "phone": "call",
    "dedupe": "content_copy",
    "summary": "bar_chart",
    "details": "list_alt",
}


def inject_css() -> None:
    """Inject the app-wide stylesheet. Safe to call once per run."""
    st.markdown(_CSS, unsafe_allow_html=True)


def _ic(name: str) -> str:
    """Inline Material Symbols icon markup for use inside raw-HTML components."""
    return f'<span class="rc-ic">{name}</span>'


def render_hero(title: str, subtitle: str, logo_data_uri: str | None = None) -> None:
    """Render the home-page hero: small logo + title + subtitle."""
    logo = f'<img class="rc-hero-logo" src="{logo_data_uri}" alt="Refinance logo" />' if logo_data_uri else ""
    st.markdown(
        f"""
        <div class="rc-hero">
          {logo}
          <div>
            <p class="rc-hero-title">{title}</p>
          </div>
        </div>
        <p class="rc-hero-sub">{subtitle}</p>
        """,
        unsafe_allow_html=True,
    )


def render_stepper(steps: List[str], active_index: int) -> None:
    """Render a horizontal stepper with Material icons. Steps before active are done."""
    html = [f'<div class="rc-stepper" role="group" aria-label="Step {active_index + 1} of {len(steps)}">']
    for i, label in enumerate(steps):
        if i < active_index:
            dot_cls, inner, lbl_cls = "rc-dot-done", _ic("check"), "rc-step-label-active"
        elif i == active_index:
            dot_cls, inner, lbl_cls = "rc-dot-active", str(i + 1), "rc-step-label-active"
        else:
            dot_cls, inner, lbl_cls = "rc-dot-todo", str(i + 1), "rc-step-label-todo"
        html.append('<div class="rc-step">')
        html.append(f'<div class="rc-dot {dot_cls}">{inner}</div>')
        html.append(f'<span class="rc-step-label {lbl_cls}">{label}</span>')
        html.append("</div>")
        if i < len(steps) - 1:
            conn = "rc-connector-done" if i < active_index else ""
            html.append(f'<div class="rc-connector {conn}"></div>')
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def option_card_body(title_icon: str, title: str, description: str) -> None:
    """Render the title + one-line description inside a home-page option card.

    Call inside a `with st.container(border=True):` block, followed by the card's
    button. A sentinel `.rc-opt` marker lets the CSS pin the button to the bottom
    so paired cards keep their buttons aligned regardless of copy length.
    """
    st.markdown(
        f"""
        <div class="rc-opt"></div>
        <div class="rc-opt-title">{_ic(title_icon)}<span>{title}</span></div>
        <p class="rc-opt-desc">{description}</p>
        """,
        unsafe_allow_html=True,
    )


def render_checklist(steps: List[str], active_index: int) -> str:
    """Return HTML for a vertical processing checklist (done / active / todo)."""
    rows = []
    for i, label in enumerate(steps):
        if i < active_index:
            cls, icon = "rc-check-done", "check_circle"
        elif i == active_index:
            cls, icon = "rc-check-active", "pending"
        else:
            cls, icon = "rc-check-todo", "radio_button_unchecked"
        rows.append(f'<div class="rc-check {cls}">{_ic(icon)}<span>{label}</span></div>')
    return "".join(rows)
