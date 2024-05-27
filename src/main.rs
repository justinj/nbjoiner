use std::collections::{HashMap, HashSet};

use prettytable::{Cell, Row, Table};
use rand::Rng;

#[derive(Default, Debug)]
struct Graph {
    edges: Vec<Vec<usize>>,
}

impl Graph {
    fn edge(&mut self, a: usize, b: usize) {
        while self.edges.len() <= a.max(b) {
            self.edges.push(Vec::new());
        }
        self.edges[a].push(b);
        self.edges[b].push(a);
    }

    fn neighbours(&self, vertex: usize) -> Vec<usize> {
        self.edges.get(vertex).cloned().unwrap_or_default()
    }
}

#[derive(Default, Debug)]
struct Planner {
    joined_tables: Vec<Relation>,
    query_graph: Graph,
}

impl Planner {
    fn join(mut self, rel: Relation) -> Self {
        for (i, t) in self.joined_tables.iter().enumerate() {
            // If there is a common column between the current relation and the
            // new relation, add an edge between them.
            let t_cols: HashSet<_> = t.col_names.iter().cloned().collect();
            if rel.col_names.iter().any(|c| t_cols.contains(c)) {
                self.query_graph.edge(self.joined_tables.len(), i);
            }
        }
        self.joined_tables.push(rel);
        self
    }

    fn plan(mut self) -> Vec<Relation> {
        let mut plan = vec![];
        let mut remaining: HashSet<_> = (0..self.joined_tables.len()).collect();
        // Grab an unjoined relation.
        while let Some(next) = remaining.iter().next() {
            // Expand outwards adding relations that are connected to the
            // current result.
            let mut frontier = vec![*next];
            while let Some(relation) = frontier.pop() {
                remaining.remove(&relation);
                plan.push(relation);
                frontier.extend(
                    self.query_graph
                        .neighbours(relation)
                        .into_iter()
                        .filter(|n| remaining.contains(n)),
                );
            }
        }

        plan.into_iter()
            .map(|i| std::mem::take(&mut self.joined_tables[i]))
            .collect()
    }
}

#[derive(Debug, Default)]
struct Relation {
    col_names: Vec<String>,
    data: Vec<Vec<i64>>,
}

impl Relation {
    fn new(col_names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            col_names: col_names.into_iter().map(|x| x.into()).collect(),
            data: Vec::new(),
        }
    }

    fn new_with_data(
        col_names: impl IntoIterator<Item = impl Into<String>>,
        data: impl IntoIterator<Item = Vec<i64>>,
    ) -> Self {
        Self {
            col_names: col_names.into_iter().map(|x| x.into()).collect(),
            data: data.into_iter().collect(),
        }
    }

    fn row(mut self, row: impl IntoIterator<Item = i64>) -> Self {
        self.data.push(row.into_iter().collect());
        self
    }

    fn rows(mut self, rows: impl IntoIterator<Item = impl IntoIterator<Item = i64>>) -> Self {
        self.data = rows.into_iter().map(|r| r.into_iter().collect()).collect();
        self
    }

    fn join(&self, other: &Relation) -> Relation {
        let common_cols = self
            .col_names
            .iter()
            .cloned()
            .filter(|col| other.col_names.contains(col))
            .collect::<Vec<_>>();

        let output_cols = self.col_names.iter().cloned().chain(
            other
                .col_names
                .iter()
                .filter(|c| !self.col_names.contains(c))
                .cloned(),
        );

        let left_key = common_cols
            .iter()
            .map(|col| self.col_names.iter().position(|c| c == col).unwrap())
            .collect::<Vec<_>>();

        let right_key = common_cols
            .iter()
            .map(|col| other.col_names.iter().position(|c| c == col).unwrap())
            .collect::<Vec<_>>();

        let mut table = HashMap::new();
        for row in self.data.iter() {
            let key = left_key.iter().map(|i| row[*i]).collect::<Vec<_>>();
            table.entry(key).or_insert_with(Vec::new).push(row);
        }

        let mut result = Vec::new();
        for row in other.data.iter() {
            if let Some(rows) = table.get(&right_key.iter().map(|i| row[*i]).collect::<Vec<_>>()) {
                for left_row in rows {
                    let mut new_row = (*left_row).clone();
                    new_row.extend(
                        row.iter()
                            .enumerate()
                            .filter(|(i, _)| !right_key.contains(i))
                            .map(|(_, v)| v)
                            .cloned(),
                    );
                    result.push(new_row);
                }
            }
        }

        Relation::new_with_data(output_cols, result)
    }

    fn print(&self) {
        let mut sorted_cols = self
            .col_names
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, x)| (x, i))
            .collect::<Vec<_>>();
        sorted_cols.sort();
        let mut table = Table::new();
        table.add_row(Row::new(
            sorted_cols
                .iter()
                .map(|(name, _)| Cell::new(name.as_str()))
                .collect(),
        ));
        for row in self.data.iter() {
            table.add_row(Row::new(
                sorted_cols
                    .iter()
                    .map(|(_, i)| Cell::new(format!("{}", row[*i]).as_str()))
                    .collect::<Vec<_>>(),
            ));
        }

        table.printstd()
    }
}

fn main() {
    let r = Relation::new(["a", "b"])
        .row([1, 2])
        .row([3, 4])
        .row([5, 6]);
    // your boat

    let s = Relation::new(["b", "c"])
        .row([2, 10])
        .row([4, 20])
        .row([6, 30]);

    println!("R:");
    r.print();
    println!("S:");
    s.print();
    println!("R join S:");
    r.join(&s).print();

    let t = Relation::new(["c", "d"])
        .row([10, 100])
        .row([20, 200])
        .row([30, 300]);

    r.join(&t).join(&s).print();

    let r2 = Relation::new(["a", "b"]).rows((0..1000).map(|i| vec![i, i * 10]));
    let s2 = Relation::new(["b", "c"]).rows((0..1000).map(|i| vec![i * 10, i * 100]));
    let t2 = Relation::new(["c", "d"]).rows((0..1000).map(|i| vec![i * 100, i * 1000]));

    r2.join(&t2).join(&s2).print();

    let s = Relation::new(["b", "c"])
        .row([2, 10])
        .row([4, 20])
        .row([6, 30]);

    r.join(&s).print();

    let t = Relation::new(["c", "d"])
        .row([10, 100])
        .row([20, 200])
        .row([30, 300]);

    r.join(&t).join(&s).print();

    let planner = Planner::default().join(r).join(t).join(s);

    let plan = planner.plan();
    let result = plan
        .into_iter()
        .reduce(|result, next| result.join(&next))
        .unwrap();

    result.print();

    let mut many_relations: Vec<_> = (0..10)
        .map(|i| {
            Relation::new_with_data(
                [format!("col_{}", i), format!("col_{}", i + 1)],
                (0..10)
                    .map(|j| vec![j * 10i64.pow(i), j * 10i64.pow(i + 1)])
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    let mut rng = rand::thread_rng();
    for i in 0..9 {
        many_relations.swap(i, rng.gen_range(i..10));
    }

    let plan = many_relations
        .into_iter()
        .fold(Planner::default(), |planner, rel| planner.join(rel))
        .plan();

    for rel in &plan {
        rel.print();
    }

    let result = plan
        .into_iter()
        .reduce(|result, next| result.join(&next))
        .unwrap();

    result.print();
}
