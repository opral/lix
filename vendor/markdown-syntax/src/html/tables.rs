//! GFM table rendering (attribute `align="…"` form).

use alloc::{format, string::String, vec::Vec};

use crate::ast::{Table, TableAlignment};

use super::inlines::render_inlines;
use super::Ctx;

/// Column alignment attribute (GFM attribute form, NOT `style=`).
pub fn table_align_attr(a: TableAlignment) -> &'static str {
    match a {
        TableAlignment::None => "",
        TableAlignment::Left => " align=\"left\"",
        TableAlignment::Right => " align=\"right\"",
        TableAlignment::Center => " align=\"center\"",
    }
}

fn alignment_for(table: &Table, col: usize) -> TableAlignment {
    table
        .alignments
        .get(col)
        .copied()
        .unwrap_or(TableAlignment::None)
}

/// Render a full GFM table. The header row drives the column count; body cells
/// beyond that width are dropped and missing cells render as empty tags.
pub fn render_table(table: &Table, ctx: &Ctx) -> String {
    let mut out = String::from("<table>\n<thead>\n<tr>\n");

    let empty: Vec<_> = Vec::new();
    let header_cells = table.rows.first().map(|r| &r.cells).unwrap_or(&empty);
    let width = header_cells.len();

    let mut head_parts: Vec<String> = Vec::with_capacity(width);
    for (col, cell) in header_cells.iter().enumerate() {
        let align = table_align_attr(alignment_for(table, col));
        head_parts.push(format!(
            "<th{align}>{}</th>",
            render_inlines(&cell.children, ctx)
        ));
    }
    out.push_str(&head_parts.join("\n"));
    out.push_str("\n</tr>\n</thead>");

    if table.rows.len() > 1 {
        out.push_str("\n<tbody>\n");
        let mut body_rows: Vec<String> = Vec::new();
        for row in &table.rows[1..] {
            let mut cell_parts: Vec<String> = Vec::with_capacity(width);
            for col in 0..width {
                let align = table_align_attr(alignment_for(table, col));
                let content = row
                    .cells
                    .get(col)
                    .map(|c| render_inlines(&c.children, ctx))
                    .unwrap_or_default();
                cell_parts.push(format!("<td{align}>{content}</td>"));
            }
            body_rows.push(format!("<tr>\n{}\n</tr>", cell_parts.join("\n")));
        }
        out.push_str(&body_rows.join("\n"));
        out.push_str("\n</tbody>\n");
    } else {
        out.push('\n');
    }

    out.push_str("</table>");
    out
}
