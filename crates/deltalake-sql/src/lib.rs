pub mod logical_plan;
pub mod parser;
pub mod planner;

#[cfg(test)]
mod tests {
    use datafusion_expr::logical_plan::LogicalPlan;

    pub fn assert_plan_eq(plan: &LogicalPlan, expected_lines: &[&str]) {
        let formatted = plan.display_indent().to_string();
        let actual_lines: Vec<_> = formatted.trim().lines().collect();
        assert_eq!(
            &actual_lines, expected_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    }
}
