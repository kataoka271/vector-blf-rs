pub struct Table {
    headers: Vec<&'static str>,
    right_align: Vec<bool>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(columns: &[(&'static str, bool)]) -> Self {
        Self {
            headers: columns.iter().map(|(h, _)| *h).collect(),
            right_align: columns.iter().map(|(_, r)| *r).collect(),
            rows: Vec::new(),
        }
    }

    pub fn push(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    pub fn print(&self) {
        let n = self.headers.len();
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.len()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate().take(n) {
                widths[i] = widths[i].max(cell.len());
            }
        }

        let fmt = |s: &str, i: usize| -> String {
            let w = widths[i];
            if self.right_align[i] {
                format!("{s:>w$}")
            } else {
                format!("{s:<w$}")
            }
        };

        let header: Vec<String> = (0..n).map(|i| fmt(self.headers[i], i)).collect();
        println!("{}", header.join("  "));

        let sep: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
        println!("{}", sep.join("--"));

        for row in &self.rows {
            let line: String = (0..n)
                .map(|i| {
                    let cell = row.get(i).map(|s| s.as_str()).unwrap_or("");
                    if i + 1 == n {
                        cell.to_string()
                    } else {
                        fmt(cell, i)
                    }
                })
                .collect::<Vec<_>>()
                .join("  ");
            println!("{}", line.trim_end());
        }
    }
}
