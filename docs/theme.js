/**
 * Light and dark theme and the sticky header, shared by Pitwall's pages (app.js and monthly.js).
 *
 * Same behaviour as the rest of baizakisaev.com: the page follows the system setting until the
 * visitor uses the header switch; that choice is saved as "agt-theme", the key every page of the
 * site reads, so it follows them across the site. Each page's <head> applies the saved theme
 * before the first paint.
 */

const systemDark = matchMedia("(prefers-color-scheme: dark)");

/** "light" or "dark": the visitor's choice if they made one, otherwise their system's. */
export const theme = () =>
  document.documentElement.getAttribute("data-theme") ||
  (systemDark.matches ? "dark" : "light");

/** A value from the stylesheet's design tokens (e.g. "--muted"), for charts and canvases. */
export const token = (name) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();

const SUN =
  '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><circle cx="12" cy="12" r="4" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
const MOON =
  '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/></svg>';

/**
 * Wires up the theme switch `button`. `onChange` runs after every theme change (the switch, or
 * the system setting when no choice was made), so the page can redraw its charts.
 */
export function setupTheme(button, onChange) {
  // The button shows the icon of the theme it switches to.
  const paint = () => {
    const next = theme() === "dark" ? "light" : "dark";
    button.innerHTML = next === "light" ? SUN : MOON;
    button.setAttribute("aria-label", `Switch to the ${next} theme`);
  };
  paint();
  button.addEventListener("click", () => {
    const next = theme() === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    try {
      localStorage.setItem("agt-theme", next);
    } catch {
      // Storage blocked: the theme still switches for this visit.
    }
    paint();
    onChange();
  });
  systemDark.addEventListener("change", () => {
    if (document.documentElement.hasAttribute("data-theme")) return;
    paint();
    onChange();
  });
}

/** Gives the sticky header its bottom line once the page scrolls under it. */
export function setupHeader(header) {
  const onScroll = () => header.classList.toggle("is-scrolled", scrollY > 4);
  addEventListener("scroll", onScroll, { passive: true });
  onScroll();
}
