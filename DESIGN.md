# Design

A calm, professional product UI for an internal refinance-lead data-cleaning tool.
Built on Streamlit; the visual system moves decisively away from default Streamlit
chrome (red accent, generic spacing, emoji) toward a quiet, legible instrument.
Real Google **Material Symbols** icons are used throughout — never emoji.

## Theme

Light, single theme. The tool is used in a focused desktop work session; a light
surface keeps dense tables and counts maximally legible. Restrained color strategy:
tinted neutrals plus one blue accent reserved for state and primary actions.

## Color

Defined as CSS variables in `ui_theme.py` and mirrored in `.streamlit/config.toml`.

- `--rc-primary` `#2563EB` — primary actions, active step, done state, links.
- `--rc-primary-dark` `#1D4ED8` — primary hover.
- `--rc-ink` `#12203A` — headings and body text (≥ 4.5:1 on all app surfaces).
- `--rc-body` `#334155` — secondary/body prose (meets AA on white and `--rc-bg-soft`).
- `--rc-muted` `#5B6B7F` — captions/labels only (AA on white; never load-bearing).
- `--rc-border` `#E2E8F0` — hairline borders.
- `--rc-bg` `#F7F9FC` — app background (page).
- `--rc-surface` `#FFFFFF` — cards, tiles, panels.
- `--rc-bg-soft` `#EEF2F7` — secondary fills (todo step dots, neutral badge).
- State: success `#047857`, warning `#B45309`, danger `#B91C1C`, plus soft tints
  (`-bg` variants) for alert backgrounds. Status is always paired with an icon or
  text label, never color alone.

## Typography

One family: the system UI sans (`system-ui` stack) that Streamlit ships, tuned by
weight rather than paired with a display face — correct for product UI. Fixed rem
scale (not fluid): page title ~1.9rem, section headings ~1.15–1.25rem, body 1rem,
captions 0.85rem. Headings 700 with `-0.01em` tracking and `text-wrap: balance`.
Tabular figures (`font-variant-numeric: tabular-nums`) on all metrics and count
tables so digits align.

## Iconography

Google Material Symbols (Rounded), delivered by Streamlit's `:material/<name>:`
directive in markdown/labels and the `icon=` argument on buttons, download buttons,
and alerts. A single weight/fill for consistency. Icon vocabulary:
- Navigation: `home`, `arrow_back`, `arrow_forward`, `restart_alt`, `delete`.
- Actions: `play_arrow` (run), `download`, `folder_zip` (ZIP).
- Workflow/state: `bolt` (fast), `description` (1 file), `folder_open` (5 files),
  `check_circle` (done/success), `pending` (in-progress), `radio_button_unchecked`
  (todo), `mop`/`cleaning_services` (clean), `filter_alt` (filter), `celebration`
  (complete), `warning`, `insights`/`bar_chart` (results), `table_rows` (data).

## Components

- **Metric tile**: white surface, 1px border, 14px radius, soft shadow; label in
  `--rc-muted` 600, value in `--rc-ink` with tabular figures. Used in equal-width rows.
- **Home workflow card**: white surface card with a role badge (Fast / Step-by-step),
  title, one-line description, and a full-width button beneath.
- **Stepper**: horizontal done/active/todo dots with connectors; done and active use
  `--rc-primary`. Screen-reader text announces "step N of M".
- **Buttons**: 10px radius, 600 weight, 1px border; primary is solid `--rc-primary`.
  Subtle 1px lift + shadow on hover; visible focus ring; :active removes the lift.
- **File uploader**: dashed 1.5px border on a faint tinted fill, hover brightens border.
- **Dataframe**: 12px radius, clipped overflow, hairline border.
- **Alerts**: Streamlit info/success/warning/error with Material `icon=`.

## Motion

150–220ms, ease-out. Motion conveys state only (button hover/press, step progress,
result arrival). No orchestrated page-load sequences. A single subtle fade-up on the
results header is the one moment of arrival warmth. All motion is disabled under
`prefers-reduced-motion: reduce`.

## Layout

Centered content column, max-width ~1140px. Home uses a 2×2 card grid (Fast row,
Classic row) that collapses to one column on narrow viewports. Responsive behavior is
structural (grid columns collapse), not fluid typography. Focused-session spacing:
generous but not sparse; deliberate vertical rhythm between sections.
