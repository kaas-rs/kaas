//! `gen-api-matrix` + `check-docs-drift` — the book's honesty levers.
//!
//! `gen-api-matrix` renders `docs/src/compat/api-matrix.md` from
//! `kaas_codec::api::registry::ALL` (the same table the ApiVersions
//! response is built from), joined with the doc-level metadata below
//! (display name, domain page, KIP cross-refs). Key + version facts can
//! therefore never drift from the wire surface; the metadata join is
//! exhaustiveness-checked in both directions and fails the build when a
//! registry key gains or loses a row.
//!
//! `check-docs-drift` mirrors `check-crd-drift` (regenerate + `git diff
//! --exit-code`) and additionally scans every book page for
//! `crates/…` / `bins/…`-style source citations, failing on paths that
//! don't exist in the tree.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use kaas_codec::api::registry::ALL;

/// Doc-level metadata for one registered API key. `page` is the Part II
/// domain page (under `docs/src/compat/api/`) whose `#<lowercase name>`
/// anchor documents the key; `kips` cross-references the KIP pages.
struct ApiDoc {
    key: i16,
    name: &'static str,
    page: &'static str,
    kips: &'static [u16],
}

/// KIPs that have their own page under `docs/src/compat/kip/`
/// (implemented + partial). Everything else in the tracked set links to
/// the non-goals page.
const KIPS_WITH_PAGES: &[u16] = &[
    482, 516, 107, 195, 339, 546, 290, 800, 13, 371, 58, 354, 32, 98, 360, 447,
    700, // implemented
    101, 219, 345, 394, 554, // partial
];

const API_DOCS: &[ApiDoc] = &[
    ApiDoc {
        key: 0,
        name: "Produce",
        page: "produce-fetch",
        kips: &[98, 360, 32],
    },
    ApiDoc {
        key: 1,
        name: "Fetch",
        page: "produce-fetch",
        kips: &[98, 227],
    },
    ApiDoc {
        key: 2,
        name: "ListOffsets",
        page: "produce-fetch",
        kips: &[32],
    },
    ApiDoc {
        key: 3,
        name: "Metadata",
        page: "produce-fetch",
        kips: &[516],
    },
    ApiDoc {
        key: 8,
        name: "OffsetCommit",
        page: "consumer-groups",
        kips: &[],
    },
    ApiDoc {
        key: 9,
        name: "OffsetFetch",
        page: "consumer-groups",
        kips: &[447],
    },
    ApiDoc {
        key: 10,
        name: "FindCoordinator",
        page: "consumer-groups",
        kips: &[],
    },
    ApiDoc {
        key: 11,
        name: "JoinGroup",
        page: "consumer-groups",
        kips: &[345, 394, 800],
    },
    ApiDoc {
        key: 12,
        name: "Heartbeat",
        page: "consumer-groups",
        kips: &[345],
    },
    ApiDoc {
        key: 13,
        name: "LeaveGroup",
        page: "consumer-groups",
        kips: &[345, 800],
    },
    ApiDoc {
        key: 14,
        name: "SyncGroup",
        page: "consumer-groups",
        kips: &[345],
    },
    ApiDoc {
        key: 15,
        name: "DescribeGroups",
        page: "consumer-groups",
        kips: &[],
    },
    ApiDoc {
        key: 16,
        name: "ListGroups",
        page: "consumer-groups",
        kips: &[],
    },
    ApiDoc {
        key: 17,
        name: "SaslHandshake",
        page: "auth",
        kips: &[],
    },
    ApiDoc {
        key: 18,
        name: "ApiVersions",
        page: "cluster-misc",
        kips: &[482],
    },
    ApiDoc {
        key: 19,
        name: "CreateTopics",
        page: "topics-configs",
        kips: &[516],
    },
    ApiDoc {
        key: 20,
        name: "DeleteTopics",
        page: "topics-configs",
        kips: &[],
    },
    ApiDoc {
        key: 21,
        name: "DeleteRecords",
        page: "topics-configs",
        kips: &[107],
    },
    ApiDoc {
        key: 22,
        name: "InitProducerId",
        page: "transactions",
        kips: &[98, 360],
    },
    ApiDoc {
        key: 24,
        name: "AddPartitionsToTxn",
        page: "transactions",
        kips: &[98],
    },
    ApiDoc {
        key: 25,
        name: "AddOffsetsToTxn",
        page: "transactions",
        kips: &[98, 447],
    },
    ApiDoc {
        key: 26,
        name: "EndTxn",
        page: "transactions",
        kips: &[98],
    },
    ApiDoc {
        key: 27,
        name: "WriteTxnMarkers",
        page: "transactions",
        kips: &[98],
    },
    ApiDoc {
        key: 28,
        name: "TxnOffsetCommit",
        page: "transactions",
        kips: &[447],
    },
    ApiDoc {
        key: 29,
        name: "DescribeAcls",
        page: "acls-quotas",
        kips: &[290],
    },
    ApiDoc {
        key: 30,
        name: "CreateAcls",
        page: "acls-quotas",
        kips: &[290],
    },
    ApiDoc {
        key: 31,
        name: "DeleteAcls",
        page: "acls-quotas",
        kips: &[290],
    },
    ApiDoc {
        key: 32,
        name: "DescribeConfigs",
        page: "topics-configs",
        kips: &[],
    },
    ApiDoc {
        key: 34,
        name: "AlterReplicaLogDirs",
        page: "cluster-misc",
        kips: &[],
    },
    ApiDoc {
        key: 35,
        name: "DescribeLogDirs",
        page: "cluster-misc",
        kips: &[],
    },
    ApiDoc {
        key: 36,
        name: "SaslAuthenticate",
        page: "auth",
        kips: &[],
    },
    ApiDoc {
        key: 37,
        name: "CreatePartitions",
        page: "topics-configs",
        kips: &[195],
    },
    ApiDoc {
        key: 42,
        name: "DeleteGroups",
        page: "consumer-groups",
        kips: &[],
    },
    ApiDoc {
        key: 44,
        name: "IncrementalAlterConfigs",
        page: "topics-configs",
        kips: &[339],
    },
    ApiDoc {
        key: 47,
        name: "OffsetDelete",
        page: "consumer-groups",
        kips: &[],
    },
    ApiDoc {
        key: 48,
        name: "DescribeClientQuotas",
        page: "acls-quotas",
        kips: &[546],
    },
    ApiDoc {
        key: 49,
        name: "AlterClientQuotas",
        page: "acls-quotas",
        kips: &[546],
    },
    ApiDoc {
        key: 50,
        name: "DescribeUserScramCredentials",
        page: "acls-quotas",
        kips: &[554],
    },
    ApiDoc {
        key: 51,
        name: "AlterUserScramCredentials",
        page: "acls-quotas",
        kips: &[554],
    },
    ApiDoc {
        key: 60,
        name: "DescribeCluster",
        page: "cluster-misc",
        kips: &[700],
    },
];

/// Apache Kafka 3.7 surface kaas does not serve. `(key, name, note)` —
/// rendered as the gap table so the matrix stays honest about absences.
const GAPS: &[(i16, &str, &str)] = &[
    (
        23,
        "OffsetForLeaderEpoch",
        "[KIP-101](kip/kip-101.md) partial — storage-side lookup returns the `(-1,-1)` sentinel; key unregistered. Open follow-up.",
    ),
    (
        33,
        "AlterConfigs (legacy)",
        "Superseded by [IncrementalAlterConfigs](api/topics-configs.md#incrementalalterconfigs) (key 44) but still served by Apache 3.7. Open follow-up.",
    ),
];

fn kip_link(kip: u16) -> String {
    if KIPS_WITH_PAGES.contains(&kip) {
        format!("[KIP-{kip}](kip/kip-{kip}.md)")
    } else {
        format!("[KIP-{kip}](non-goals.md)")
    }
}

/// Render the full markdown document.
pub fn render() -> Result<String> {
    // Exhaustiveness both ways: every registry key has metadata, every
    // metadata row has a registry key.
    let reg_keys: BTreeSet<i16> = ALL.iter().map(|s| s.key).collect();
    let doc_keys: BTreeSet<i16> = API_DOCS.iter().map(|d| d.key).collect();
    if reg_keys != doc_keys {
        let missing: Vec<_> = reg_keys.difference(&doc_keys).collect();
        let stale: Vec<_> = doc_keys.difference(&reg_keys).collect();
        bail!(
            "api-matrix metadata out of sync with registry: \
             missing metadata for keys {missing:?}, stale metadata for keys {stale:?} \
             (edit API_DOCS in xtask/src/api_matrix.rs)"
        );
    }

    let mut out = String::new();
    out.push_str(
        "# API support matrix\n\n\
         <!-- GENERATED FILE — do not edit. Regenerate with `cargo xtask gen-api-matrix`;\n\
         `cargo xtask check-docs-drift` (CI) fails when this file drifts from\n\
         crates/kaas-codec/src/api/registry.rs. -->\n\n",
    );
    out.push_str(&format!(
        "kaas registers **{} Kafka API keys**. This table is generated from the\n\
         `ApiSpec` registry (`crates/kaas-codec/src/api/registry.rs`) — the same table\n\
         that builds the ApiVersions response — so the version ranges below are the\n\
         wire truth, not documentation aspiration. \"Flexible\" is the first version\n\
         using KIP-482 flexible encoding (see [Wire protocol & framing](wire-protocol.md)).\n\n",
        ALL.len()
    ));
    out.push_str("| Key | API | Versions | Flexible | KIPs | Reference |\n");
    out.push_str("|--:|---|---|---|---|---|\n");

    let mut specs: Vec<_> = ALL.iter().collect();
    specs.sort_by_key(|s| s.key);
    for spec in specs {
        let doc = API_DOCS
            .iter()
            .find(|d| d.key == spec.key)
            .with_context(|| format!("metadata for key {} vanished", spec.key))?;
        let flexible = match spec.min_flexible {
            Some(v) => format!("v{v}+"),
            None => "—".to_owned(),
        };
        let kips = if doc.kips.is_empty() {
            "—".to_owned()
        } else {
            doc.kips
                .iter()
                .map(|k| kip_link(*k))
                .collect::<Vec<_>>()
                .join(" · ")
        };
        let anchor = doc.name.to_lowercase();
        out.push_str(&format!(
            "| {} | {} | v{}–v{} | {} | {} | [{}](api/{}.md#{}) |\n",
            spec.key,
            doc.name,
            spec.min_version,
            spec.max_version,
            flexible,
            kips,
            doc.name,
            doc.page,
            anchor
        ));
    }

    out.push_str(
        "\n## Apache 3.7 keys kaas does not serve\n\n\
         Clients discover the served surface via ApiVersions, so an absent key is a\n\
         clean \"unsupported\", not an error path. Each absence is either a tracked\n\
         follow-up or a documented [non-goal](non-goals.md):\n\n",
    );
    out.push_str("| Key | API | Status |\n|--:|---|---|\n");
    for (key, name, note) in GAPS {
        out.push_str(&format!("| {key} | {name} | {note} |\n"));
    }
    out.push_str(
        "\nInter-broker/KRaft keys (LeaderAndIsr, StopReplica, UpdateMetadata,\n\
         ControlledShutdown, the quorum/Envelope family), delegation-token keys, and\n\
         tiered-storage-only surfaces are deliberately absent — see\n\
         [Non-goals](non-goals.md).\n",
    );
    Ok(out)
}

pub fn generate(repo_root: &Path) -> Result<()> {
    let path = repo_root.join("docs/src/compat/api-matrix.md");
    let body = render()?;
    let tmp = path.with_extension("md.tmp");
    fs::write(&tmp, body.as_bytes()).with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, &path).with_context(|| format!("renaming into {}", path.display()))?;
    eprintln!("gen-api-matrix: wrote {}", path.display());
    Ok(())
}

/// Verify every registered API key's `## <Name>` heading exists
/// exactly once across the Part II domain pages. mdbook-linkcheck
/// 0.7.7 does not validate fragments, so without this check a renamed
/// heading would silently break every matrix deep-link to it.
pub fn check_api_anchors(repo_root: &Path) -> Result<()> {
    let api_dir = repo_root.join("docs/src/compat/api");
    let mut headings: Vec<(String, String)> = Vec::new(); // (page, heading)
    for entry in fs::read_dir(&api_dir).with_context(|| format!("reading {}", api_dir.display()))? {
        let path = entry?.path();
        if path.extension().is_none_or(|e| e != "md") {
            continue;
        }
        let page = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_owned();
        for line in fs::read_to_string(&path)?.lines() {
            if let Some(h) = line.strip_prefix("## ") {
                headings.push((page.clone(), h.trim().to_owned()));
            }
        }
    }
    let mut bad = Vec::new();
    for doc in API_DOCS {
        let hits: Vec<_> = headings.iter().filter(|(_, h)| h == doc.name).collect();
        match hits.len() {
            1 if hits[0].0 == doc.page => {}
            1 => bad.push(format!(
                "key {} ({}) documented on page {:?}, matrix links to {:?}",
                doc.key, doc.name, hits[0].0, doc.page
            )),
            0 => bad.push(format!(
                "key {} ({}): no `## {}` heading on any compat/api page (matrix links to {}.md#{})",
                doc.key,
                doc.name,
                doc.name,
                doc.page,
                doc.name.to_lowercase()
            )),
            n => bad.push(format!(
                "key {} ({}): `## {}` appears {n} times (must be exactly once)",
                doc.key, doc.name, doc.name
            )),
        }
    }
    if !bad.is_empty() {
        bail!("API anchor check failed:\n  {}", bad.join("\n  "));
    }
    eprintln!(
        "check-docs-drift: API anchor check clean ({} keys)",
        API_DOCS.len()
    );
    Ok(())
}

/// Scan every book page for `crates/…`, `bins/…`, `scripts/…`,
/// `deploy/…`, `proto/…`, `xtask/…` citations and fail on paths that
/// don't exist. A citation is a match ending in a known source-file
/// extension (checked as a file) or a trailing `/` (checked as a
/// directory); matches truncated by wildcard/placeholder characters
/// degrade to their parent directory and are checked as such.
pub fn scan_source_paths(repo_root: &Path) -> Result<()> {
    let src = repo_root.join("docs/src");
    let re = regex::Regex::new(r"(?:crates|bins|scripts|deploy|proto|xtask)/[A-Za-z0-9_.\-/]*")
        .context("compiling citation regex")?;
    const FILE_EXTS: &[&str] = &[
        "rs", "toml", "proto", "sh", "yaml", "yml", "json", "md", "txt",
    ];

    let mut pages = Vec::new();
    collect_md(&src, &mut pages)?;
    let mut bad: Vec<String> = Vec::new();
    for page in &pages {
        let text = fs::read_to_string(page)?;
        for m in re.find_iter(&text) {
            let cited = m.as_str().trim_end_matches(['.', ',', ';', ':', ')', '-']);
            let candidate = if cited.ends_with('/') {
                cited.trim_end_matches('/')
            } else if Path::new(cited)
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| FILE_EXTS.contains(&e))
            {
                cited
            } else {
                continue; // prose fragment, not a checkable citation
            };
            if !repo_root.join(candidate).exists() {
                bad.push(format!(
                    "{}: cites {candidate:?} which does not exist",
                    page.strip_prefix(repo_root).unwrap_or(page).display()
                ));
            }
        }
    }
    if !bad.is_empty() {
        bail!(
            "stale source citations in the book:\n  {}",
            bad.join("\n  ")
        );
    }
    eprintln!(
        "check-docs-drift: source-path scan clean ({} pages)",
        pages.len()
    );
    Ok(())
}

fn collect_md(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            collect_md(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
    Ok(())
}
