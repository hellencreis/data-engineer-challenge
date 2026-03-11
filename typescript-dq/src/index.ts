import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

type Row = Record<string, string>;

type ColumnSummary = {
  nullCount: number;
  completenessPct: number;
};

type RuleResult = {
  rule: string;
  failedRows: number;
};

type Report = {
  inputFile: string;
  generatedAt: string;
  totalRows: number;
  expectedColumns: string[];
  missingColumns: string[];
  columnSummary: Record<string, ColumnSummary>;
  duplicateOrderIds: number;
  rules: RuleResult[];
};

const EXPECTED_COLUMNS = [
  "order_id",
  "order_ts",
  "product_id",
  "customer_id",
  "quantity",
  "unit_price",
  "currency",
  "rate_to_usd",
  "revenue_usd"
];

function getArg(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  if (index === -1 || index + 1 >= process.argv.length) return undefined;
  return process.argv[index + 1];
}

function isBlank(value: unknown): boolean {
  return value === undefined || value === null || String(value).trim() === "";
}

function toNumber(value: string): number | null {
  if (isBlank(value)) return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
}

function ensureDir(dirPath: string): void {
  fs.mkdirSync(dirPath, { recursive: true });
}

function loadCsv(filePath: string): Row[] {
  const raw = fs.readFileSync(filePath, "utf-8");
  return parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true
  }) as Row[];
}

function buildColumnSummary(rows: Row[], headers: string[]): Record<string, ColumnSummary> {
  const summary: Record<string, ColumnSummary> = {};

  for (const header of headers) {
    const nullCount = rows.filter((row) => isBlank(row[header])).length;
    const completenessPct =
      rows.length === 0 ? 100 : Number((((rows.length - nullCount) / rows.length) * 100).toFixed(2));

    summary[header] = {
      nullCount,
      completenessPct
    };
  }

  return summary;
}

function countDuplicateOrderIds(rows: Row[]): number {
  const counts = new Map<string, number>();

  for (const row of rows) {
    const orderId = row["order_id"];
    if (isBlank(orderId)) continue;
    counts.set(orderId, (counts.get(orderId) ?? 0) + 1);
  }

  let duplicates = 0;
  for (const count of counts.values()) {
    if (count > 1) duplicates += count - 1;
  }
  return duplicates;
}

function evaluateRules(rows: Row[]): RuleResult[] {
  const results: RuleResult[] = [];

  const quantityInvalid = rows.filter((r) => {
    const n = toNumber(r["quantity"]);
    return n === null || n <= 0;
  }).length;

  const unitPriceInvalid = rows.filter((r) => {
    const n = toNumber(r["unit_price"]);
    return n === null || n < 0;
  }).length;

  const rateInvalid = rows.filter((r) => {
    const n = toNumber(r["rate_to_usd"]);
    return n === null || n <= 0;
  }).length;

  const revenueInvalid = rows.filter((r) => {
    const n = toNumber(r["revenue_usd"]);
    return n === null || n < 0;
  }).length;

  const missingCurrency = rows.filter((r) => isBlank(r["currency"])).length;
  const missingTimestamp = rows.filter((r) => isBlank(r["order_ts"])).length;

  results.push({ rule: "quantity must be > 0", failedRows: quantityInvalid });
  results.push({ rule: "unit_price must be >= 0", failedRows: unitPriceInvalid });
  results.push({ rule: "rate_to_usd must be > 0", failedRows: rateInvalid });
  results.push({ rule: "revenue_usd must be >= 0", failedRows: revenueInvalid });
  results.push({ rule: "currency must be present", failedRows: missingCurrency });
  results.push({ rule: "order_ts must be present", failedRows: missingTimestamp });

  return results;
}

function buildMarkdown(report: Report): string {
  const lines: string[] = [];

  lines.push("# Data Quality Report");
  lines.push("");
  lines.push(`- **Input file:** ${report.inputFile}`);
  lines.push(`- **Generated at:** ${report.generatedAt}`);
  lines.push(`- **Total rows:** ${report.totalRows}`);
  lines.push("");

  lines.push("## Schema Validation");
  lines.push("");
  lines.push(`- **Expected columns:** ${report.expectedColumns.join(", ")}`);
  lines.push(`- **Missing columns:** ${report.missingColumns.length ? report.missingColumns.join(", ") : "None"}`);
  lines.push("");

  lines.push("## Column Completeness");
  lines.push("");
  lines.push("| Column | Null Count | Completeness % |");
  lines.push("|---|---:|---:|");
  for (const [column, stats] of Object.entries(report.columnSummary)) {
    lines.push(`| ${column} | ${stats.nullCount} | ${stats.completenessPct} |`);
  }
  lines.push("");

  lines.push("## Duplicate Checks");
  lines.push("");
  lines.push(`- **Duplicate order_id rows:** ${report.duplicateOrderIds}`);
  lines.push("");

  lines.push("## Rule Validation");
  lines.push("");
  lines.push("| Rule | Failed Rows |");
  lines.push("|---|---:|");
  for (const rule of report.rules) {
    lines.push(`| ${rule.rule} | ${rule.failedRows} |`);
  }
  lines.push("");

  return lines.join("\n");
}

function main(): void {
  const input = getArg("--input");
  const out = getArg("--out");

  if (!input || !out) {
    console.error("Usage: npm run dq -- --input <path-to-csv> --out <output-folder>");
    process.exit(1);
  }

  if (!fs.existsSync(input)) {
    console.error(`Input file not found: ${input}`);
    process.exit(1);
  }

  const rows = loadCsv(input);
  const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
  const missingColumns = EXPECTED_COLUMNS.filter((col) => !headers.includes(col));

  const report: Report = {
    inputFile: input,
    generatedAt: new Date().toISOString(),
    totalRows: rows.length,
    expectedColumns: EXPECTED_COLUMNS,
    missingColumns,
    columnSummary: buildColumnSummary(rows, headers),
    duplicateOrderIds: countDuplicateOrderIds(rows),
    rules: evaluateRules(rows)
  };

  ensureDir(out);

  const jsonPath = path.join(out, "dq_report.json");
  const mdPath = path.join(out, "dq_report.md");

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf-8");
  fs.writeFileSync(mdPath, buildMarkdown(report), "utf-8");

  console.log(`DQ report generated:
- ${jsonPath}
- ${mdPath}`);
}

main();