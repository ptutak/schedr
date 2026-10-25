use pretty_assertions::assert_eq;

use super::*;

const PROJECT: &str = "\
# schedr: Project Decisions

Intro text.

## Goal

A scheduler.

### Constraints that follow

- one

## Architecture

```toml
## not a heading
[a]
```

### Components

#### Roles

Text.
";

#[test]
fn heading_levels() {
    assert_eq!(heading_level("# A"), Some(1));
    assert_eq!(heading_level("### Components"), Some(3));
    assert_eq!(heading_level("######"), Some(6));
    assert_eq!(heading_level("####### seven"), None);
    assert_eq!(heading_level("#hashtag"), None);
    assert_eq!(heading_level("text # not"), None);
    assert_eq!(heading_level(""), None);
}

#[test]
fn promote_headings_raises_levels_outside_code_only() {
    let text = "# Keep\n## Two\n```\n### in code\n```\n~~~\n## tilde code\n~~~\n#### Four\n";
    assert_eq!(
        promote_headings(text),
        "# Keep\n# Two\n```\n### in code\n```\n~~~\n## tilde code\n~~~\n### Four\n"
    );
}

#[test]
fn promote_headings_keeps_a_missing_trailing_newline() {
    assert_eq!(promote_headings("## A"), "# A");
}

#[test]
fn split_chapters_makes_one_page_per_second_level_heading() {
    let chapters = split_chapters(PROJECT);
    let titles: Vec<&str> = chapters.iter().map(|c| c.title.as_str()).collect();
    assert_eq!(
        titles,
        ["schedr: Project Decisions", "Goal", "Architecture"]
    );
    assert_eq!(
        chapters[0].page,
        "# schedr: Project Decisions\n\nIntro text.\n"
    );
    assert_eq!(
        chapters[1].page,
        "# Goal\n\nA scheduler.\n\n## Constraints that follow\n\n- one\n\n"
    );
    assert_eq!(
        chapters[2].page,
        "# Architecture\n\n```toml\n## not a heading\n[a]\n```\n\n## Components\n\n### Roles\n\nText.\n"
    );
}

#[test]
fn split_chapters_without_an_intro_starts_with_the_first_chapter() {
    let chapters = split_chapters("## Only\n\nBody.\n");
    assert_eq!(chapters.len(), 1);
    assert_eq!(chapters[0].title, "Only");
}

#[test]
fn split_chapters_titles_an_intro_without_a_heading() {
    let chapters = split_chapters("Some text.\n\n## Next\n");
    assert_eq!(chapters[0].title, "Introduction");
    assert_eq!(chapters[0].page, "# Introduction\n\nSome text.\n");
}

#[test]
fn slugs_are_lowercase_with_single_dashes() {
    assert_eq!(
        slug("schedr: Project Decisions"),
        "schedr-project-decisions"
    );
    assert_eq!(
        slug("Runs: manual runs, backfill and pausing"),
        "runs-manual-runs-backfill-and-pausing"
    );
    assert_eq!(slug("  Spaces  "), "spaces");
    assert_eq!(slug("Ünïcode & more!"), "n-code-more");
}

fn crate_doc(name: &str, lib: &str, description: &str) -> CrateDoc {
    CrateDoc {
        name: name.into(),
        lib_name: lib.into(),
        description: description.into(),
    }
}

#[test]
fn rust_api_page_links_each_crate() {
    let page = rust_api_page(&[
        crate_doc("schedr", "schedr_bin", "The binary."),
        crate_doc("schedr-model", "schedr_model", "Model <DAG JSON> & states."),
    ]);
    assert!(page.starts_with("# Rust API\n"));
    assert!(page.contains(
        "<li><a href=\"../../rust/schedr_bin/index.html\"><code>schedr</code></a>: The binary.</li>"
    ));
    assert!(page.contains("Model &lt;DAG JSON&gt; &amp; states."));
    assert!(page.contains("```{raw} html\n<ul>\n"));
    assert!(page.ends_with("</ul>\n```\n"));
}

#[test]
fn docs_sources_contains_every_page() {
    let files = docs_sources(
        PROJECT,
        "# readme\n",
        "# plan\n",
        &[crate_doc("a", "a", "")],
    );
    let paths: Vec<String> = files.iter().map(|f| f.path.display().to_string()).collect();
    assert_eq!(
        paths,
        [
            "docs/_generated/01-overview.md",
            "docs/_generated/06-plan.md",
            "docs/_generated/07-api/01-python.md",
            "docs/_generated/07-api/02-rust.md",
            "docs/_generated/05-design/00-schedr-project-decisions.md",
            "docs/_generated/05-design/01-goal.md",
            "docs/_generated/05-design/02-architecture.md",
        ]
    );
    assert_eq!(files[0].contents, "# readme\n");
    assert_eq!(files[1].contents, "# plan\n");
    assert!(files[5].contents.starts_with("# Goal\n"));
}

const METADATA: &str = r#"{"packages": [
  {"name": "xtask", "publish": [], "description": "Dev tasks.",
   "targets": [{"kind": ["lib"], "name": "xtask"}, {"kind": ["bin"], "name": "xtask"}]},
  {"name": "schedr-model", "publish": null, "description": "Model.",
   "targets": [{"kind": ["lib"], "name": "schedr_model"}]},
  {"name": "schedr", "publish": null, "description": "Binary.",
   "targets": [{"kind": ["lib"], "name": "schedr_bin"}, {"kind": ["bin"], "name": "schedr"}]},
  {"name": "only-bin", "publish": null,
   "targets": [{"kind": ["bin"], "name": "only-bin"}]},
  {"name": "dashed-lib", "publish": null,
   "targets": [{"kind": ["lib"], "name": "dashed-lib"}]}
]}"#;

#[test]
fn crates_from_metadata_keeps_published_libraries_sorted() {
    assert_eq!(
        crates_from_metadata(METADATA).unwrap(),
        vec![
            crate_doc("dashed-lib", "dashed_lib", ""),
            crate_doc("schedr", "schedr_bin", "Binary."),
            crate_doc("schedr-model", "schedr_model", "Model."),
        ]
    );
}

#[test]
fn crates_from_metadata_rejects_unexpected_input() {
    assert!(
        crates_from_metadata("not json")
            .unwrap_err()
            .starts_with("cargo metadata: ")
    );
    assert_eq!(
        crates_from_metadata("{}").unwrap_err(),
        "cargo metadata: no `packages`"
    );
}
