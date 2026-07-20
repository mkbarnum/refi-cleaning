# Product

## Register

product

## Users

A single operator or a small handful of internal operators who run refinance-lead
data cleaning, typically weekly, on a desktop browser. They know the pipeline and
the data; they are not first-time users who need hand-holding. Their context is a
focused work session: upload files, run the pipeline, download clean results, move
on. Speed to result and legibility of what was removed matter far more than
onboarding or marketing polish.

## Product Purpose

A Streamlit tool that cleans raw refinance lead files through a fixed pipeline —
bad-data filtering, TCPA/DNC suppression, zip and phone removal, master-list
suppression, cross-file dedupe, bad-state filtering, and billing dedupe — and
produces downloadable cleaned files plus a fully-attributed record of every removed
row (Step + Reason). Success is: the operator trusts the numbers, understands what
was removed and why, and finishes a batch in a few clicks.

## Brand Personality

Clean, professional, calm. Three words: trustworthy, precise, unobtrusive. The tool
should feel like a well-made instrument — the interface disappears into the task.
No flash, no ceremony. The one moment of warmth is arrival/completion feedback;
everything else stays quiet and legible.

## Anti-references

- **Default Streamlit look** — the out-of-the-box red accent, generic spacing, and
  undifferentiated widget chrome. This is the primary thing to move away from.
- Over-designed dashboards: gradients, glassmorphism, heavy motion, decorative
  hero-metric templates.
- Cold battleship-gray enterprise admin panels with no care for legibility.

## Design Principles

1. **The numbers are the product.** Counts, removal reasons, and before/after deltas
   are the most important thing on any results screen. Design serves legibility of
   the data, not decoration around it.
2. **Show what was removed and why.** Every removal is attributable (Step + Reason).
   The UI never hides the accounting; it makes it easy to review and export.
3. **Fewest clicks to a trusted result.** The fast one-click path is the default;
   the interface should never add ceremony between upload and download.
4. **Quiet by default, clear on state.** Restrained color and motion. Color and
   emphasis are reserved for state (progress, success, skipped, error), not chrome.
5. **Consistent vocabulary everywhere.** The same stage names, the same button
   shapes, the same metric tiles across the fast and classic workflows.

## Accessibility & Inclusion

Target WCAG AA for text contrast (≥4.5:1 body, ≥3:1 large/UI). Visible keyboard
focus on all interactive controls. All motion must have a `prefers-reduced-motion`
alternative. Status must never be conveyed by color alone — pair every color cue
(success, skipped, error, progress) with a text label or icon.
