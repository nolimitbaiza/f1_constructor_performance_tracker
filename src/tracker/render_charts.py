"""Render a Top-10 chart for every month, plus a small local page to browse them."""
from .report import CHARTS, REPORTS, load_gold, plot_topk_for_month

# Load months present in gold
g = load_gold()
months = (
    g["m"].dt.strftime("%Y-%m")
    .drop_duplicates()
    .sort_values()
    .tolist()
)

# Render a Top-10 PNG for every available month
for ym in months:
    plot_topk_for_month(g, ym, 10)

# Build a tiny dropdown HTML that swaps the image by month
options_js = ",".join([f"'{m}'" for m in months])
html = f"""
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>F1 Top-10 by Month</title></head>
  <body>
    <label for="m">Month:</label>
    <select id="m"></select>
    <br/><br/>
    <img id="chart" width="900"/>
    <script>
      const months=[{options_js}];
      const sel=document.getElementById('m');
      months.forEach(x=>{{const o=document.createElement('option');o.value=x;o.textContent=x;sel.appendChild(o);}});
      function set(x){{document.getElementById('chart').src='charts/top10_'+x+'.png'; sel.value=x;}}
      set(months[months.length-1]); // default to latest month
      sel.onchange=()=>set(sel.value);
    </script>
  </body>
</html>
"""
(REPORTS / "index.html").write_text(html, encoding="utf-8")

print("Rendered", len(months), "charts into", CHARTS)
print("Open:", REPORTS / "index.html")
