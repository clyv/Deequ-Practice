## 📊 Results

| Metric | Value |
|---|---|
| Months analyzed | 2025-09, 2025-10, 2025-11 |
| Total rows validated | 12,861,158 |
| Constraint checks run | 12 |
| ✅ Checks passed | 6 |
| 🚨 Checks failed | 6 |

### Real Data Quality Issues Found

| Issue | Rows Affected | % of Data |
|---|---|---|
| Invalid passenger count (outside 1–6) | 3,137,285 | 24.39% |
| Zero or negative fare amount | 975,362 | 7.58% |
| Negative fare amount | 969,118 | 7.54% |
| Zero trip distance | 359,502 | 2.80% |
| Dropoff recorded before pickup | 187,267 | 1.46% |
| Negative tip amount | 296 | 0.00% |

### Drift Detected Across 3 Months
- Mean fare amount: $19.20 → $17.12 (-10.86% change)
- Mean trip distance: 6.84 → 6.53 miles (-4.51% change)
- Passenger count completeness: 0.7490 → 0.7573
