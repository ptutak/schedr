use pretty_assertions::assert_eq;

use super::*;

#[test]
fn a_problem_lasts_until_cleared_and_is_shared_by_clones() {
    let problems = Problems::default();
    let seen = problems.clone();
    problems.set(
        ProblemKind::Round,
        true,
        "a round failed: the database is gone",
    );
    problems.set(ProblemKind::Lease, false, "the lease isn't renewed in time");
    problems.set(ProblemKind::Round, true, "a round failed: again");
    assert_eq!(
        seen.now(),
        [
            NodeProblem {
                error: false,
                message: "the lease isn't renewed in time".to_owned()
            },
            NodeProblem {
                error: true,
                message: "a round failed: again".to_owned()
            },
        ]
    );
    problems.clear(ProblemKind::Round);
    problems.clear(ProblemKind::Notifications);
    assert_eq!(seen.now().len(), 1);
}
