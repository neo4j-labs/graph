/// Gherkin .feature file parser for the openCypher TCK.

#[derive(Debug)]
pub struct Feature {
    pub name: String,
    pub scenarios: Vec<Scenario>,
}

#[derive(Debug)]
pub struct Scenario {
    pub name: String,
    pub tags: Vec<String>,
    pub setup: ScenarioSetup,
    pub query: String,
    pub parameters: Vec<(String, String)>,
    pub expected: ExpectedResult,
}

#[derive(Debug, Default)]
pub struct ScenarioSetup {
    pub graph: GraphSetup,
    pub setup_queries: Vec<String>,
}

#[derive(Debug, Default)]
pub enum GraphSetup {
    #[default]
    Empty,
    Any,
    Named(String),
}

#[derive(Debug)]
pub enum ExpectedResult {
    Success {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        ordered: bool,
    },
    Error {
        error_type: String,
        phase: String,
        detail: String,
    },
    NoResult,
}

pub fn parse_feature(content: &str) -> Feature {
    let lines: Vec<&str> = content.lines().collect();
    let mut feature_name = String::new();
    let mut scenarios = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        if let Some(name) = line.strip_prefix("Feature:") {
            feature_name = name.trim().to_string();
            i += 1;
            continue;
        }

        if line.starts_with("Scenario:") || line.starts_with("Scenario Outline:") {
            let (scenario, next_i) = parse_scenario(&lines, i);
            scenarios.push(scenario);
            i = next_i;
            continue;
        }

        i += 1;
    }

    Feature {
        name: feature_name,
        scenarios,
    }
}

fn parse_scenario(lines: &[&str], start: usize) -> (Scenario, usize) {
    let mut i = start;
    let line = lines[i].trim();
    let name = if let Some(n) = line.strip_prefix("Scenario:") {
        n.trim().to_string()
    } else if let Some(n) = line.strip_prefix("Scenario Outline:") {
        n.trim().to_string()
    } else {
        String::new()
    };
    i += 1;

    let mut setup = ScenarioSetup::default();
    let mut query = String::new();
    let mut parameters = Vec::new();
    let mut expected = ExpectedResult::NoResult;

    while i < lines.len() {
        let line = lines[i].trim();

        // Check if we've hit the next scenario
        if (line.starts_with("Scenario:") || line.starts_with("Scenario Outline:"))
            && i > start
        {
            break;
        }

        if line.starts_with("Given an empty graph") {
            setup.graph = GraphSetup::Empty;
            i += 1;
        } else if line.starts_with("Given any graph") {
            setup.graph = GraphSetup::Any;
            i += 1;
        } else if let Some(rest) = line.strip_prefix("Given the ") {
            if let Some(name) = rest.strip_suffix(" graph") {
                setup.graph = GraphSetup::Named(name.to_string());
            }
            i += 1;
        } else if line.starts_with("And having executed:") {
            i += 1;
            let (block, next_i) = read_doc_string(lines, i);
            setup.setup_queries.push(block);
            i = next_i;
        } else if line.starts_with("And parameters are:") {
            i += 1;
            let (params, next_i) = read_table(lines, i);
            for row in params {
                if row.len() >= 2 {
                    parameters.push((row[0].clone(), row[1].clone()));
                }
            }
            i = next_i;
        } else if line.starts_with("When executing query:") {
            i += 1;
            let (block, next_i) = read_doc_string(lines, i);
            query = block;
            i = next_i;
        } else if line.starts_with("Then the result should be, in any order:")
            || line.starts_with("Then the result should be (ignoring element order for lists):")
        {
            i += 1;
            let (table, next_i) = read_table(lines, i);
            if let Some((header, rows)) = table.split_first() {
                expected = ExpectedResult::Success {
                    columns: header.clone(),
                    rows: rows.to_vec(),
                    ordered: false,
                };
            }
            i = next_i;
        } else if line.starts_with("Then the result should be, in order:") {
            i += 1;
            let (table, next_i) = read_table(lines, i);
            if let Some((header, rows)) = table.split_first() {
                expected = ExpectedResult::Success {
                    columns: header.clone(),
                    rows: rows.to_vec(),
                    ordered: true,
                };
            }
            i = next_i;
        } else if line.starts_with("Then the result should be empty") {
            expected = ExpectedResult::Success {
                columns: Vec::new(),
                rows: Vec::new(),
                ordered: false,
            };
            i += 1;
        } else if let Some(rest) = line.strip_prefix("Then a ") {
            // Error expectation: "Then a SyntaxError should be raised at compile time: ..."
            let parts: Vec<&str> = rest.splitn(2, " should be raised at ").collect();
            if parts.len() == 2 {
                let error_type = parts[0].to_string();
                let rest2: Vec<&str> = parts[1].splitn(2, ':').collect();
                let phase = rest2[0].trim().to_string();
                let detail = if rest2.len() > 1 {
                    rest2[1].trim().to_string()
                } else {
                    String::new()
                };
                expected = ExpectedResult::Error {
                    error_type,
                    phase,
                    detail,
                };
            }
            i += 1;
        } else {
            i += 1;
        }
    }

    (
        Scenario {
            name,
            tags: Vec::new(),
            setup,
            query,
            parameters,
            expected,
        },
        i,
    )
}

/// Read a """ doc string """ block.
fn read_doc_string(lines: &[&str], start: usize) -> (String, usize) {
    let mut i = start;
    // Skip to opening """
    while i < lines.len() && !lines[i].trim().starts_with("\"\"\"") {
        i += 1;
    }
    i += 1; // Skip opening """

    let mut content = String::new();
    while i < lines.len() && !lines[i].trim().starts_with("\"\"\"") {
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str(lines[i].trim());
        i += 1;
    }
    i += 1; // Skip closing """

    (content, i)
}

/// Read a | col1 | col2 | table.
fn read_table(lines: &[&str], start: usize) -> (Vec<Vec<String>>, usize) {
    let mut rows = Vec::new();
    let mut i = start;

    while i < lines.len() {
        let line = lines[i].trim();
        if !line.starts_with('|') {
            break;
        }

        let cells: Vec<String> = line
            .split('|')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        rows.push(cells);
        i += 1;
    }

    (rows, i)
}
