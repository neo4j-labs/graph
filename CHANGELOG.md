<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org).

## Unreleased

## [graph_builder-v0.4.0](https://github.com/neo4j-labs/graph/tree/graph_builder-v0.4.0) - 2023-11-03

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_builder-v0.3.1...graph_builder-v0.4.0)

### Other

- release: graph_builder v0.4.0 [#116](https://github.com/neo4j-labs/graph/pull/116) ([github-actions](https://github.com/github-actions))
- Improve Adjacency List build performance [#115](https://github.com/neo4j-labs/graph/pull/115) ([s1ck](https://github.com/s1ck))
- Thread-safe edge mutation [#114](https://github.com/neo4j-labs/graph/pull/114) ([s1ck](https://github.com/s1ck))
- Add adjacency list backed graph implementation [#113](https://github.com/neo4j-labs/graph/pull/113) ([s1ck](https://github.com/s1ck))
- Bump arrow/arrow-flight to 45.0.0 and re-enable server crate [#112](https://github.com/neo4j-labs/graph/pull/112) ([s1ck](https://github.com/s1ck))
- Update GHA definitions to allow Github based publishing [#111](https://github.com/neo4j-labs/graph/pull/111) ([knutwalker](https://github.com/knutwalker))
- Add benchmark utils crate [#106](https://github.com/neo4j-labs/graph/pull/106) ([s1ck](https://github.com/s1ck))

## [graph_builder-v0.3.1](https://github.com/neo4j-labs/graph/tree/graph_builder-v0.3.1) - 2023-02-19

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_mate-v0.1.1...graph_builder-v0.3.1)

### Other

- Improve dotgraph label stats computation [#105](https://github.com/neo4j-labs/graph/pull/105) ([s1ck](https://github.com/s1ck))
- Bump gdl version to 0.2.7 [#104](https://github.com/neo4j-labs/graph/pull/104) ([s1ck](https://github.com/s1ck))
- dotgraph refactoring [#103](https://github.com/neo4j-labs/graph/pull/103) ([s1ck](https://github.com/s1ck))
- Add notebook for page_rank on wikipedia articles [#100](https://github.com/neo4j-labs/graph/pull/100) ([s1ck](https://github.com/s1ck))

## [graph_mate-v0.1.1](https://github.com/neo4j-labs/graph/tree/graph_mate-v0.1.1) - 2022-11-23

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_server-v0.2.0...graph_mate-v0.1.1)

### Other

- Add edgelist as possible input for load methods in graph_mate [#101](https://github.com/neo4j-labs/graph/pull/101) ([knutwalker](https://github.com/knutwalker))
- Use consisten release commit messages [#99](https://github.com/neo4j-labs/graph/pull/99) ([knutwalker](https://github.com/knutwalker))
- Add build badges [#98](https://github.com/neo4j-labs/graph/pull/98) ([knutwalker](https://github.com/knutwalker))
- Silence false positive warning from unused_crate_dependencies [#97](https://github.com/neo4j-labs/graph/pull/97) ([knutwalker](https://github.com/knutwalker))
- Merge GA files [#96](https://github.com/neo4j-labs/graph/pull/96) ([knutwalker](https://github.com/knutwalker))

## [graph_server-v0.2.0](https://github.com/neo4j-labs/graph/tree/graph_server-v0.2.0) - 2022-11-17

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_app-v0.2.0...graph_server-v0.2.0)

## [graph_app-v0.2.0](https://github.com/neo4j-labs/graph/tree/graph_app-v0.2.0) - 2022-11-17

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_mate-v0.1.0...graph_app-v0.2.0)

## [graph_mate-v0.1.0](https://github.com/neo4j-labs/graph/tree/graph_mate-v0.1.0) - 2022-11-17

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph-v0.3.0...graph_mate-v0.1.0)

## [graph-v0.3.0](https://github.com/neo4j-labs/graph/tree/graph-v0.3.0) - 2022-11-17

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_builder-v0.3.0...graph-v0.3.0)

## [graph_builder-v0.3.0](https://github.com/neo4j-labs/graph/tree/graph_builder-v0.3.0) - 2022-11-17

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_mate-v0.0.2...graph_builder-v0.3.0)

### Other

- Update dependencies [#95](https://github.com/neo4j-labs/graph/pull/95) ([s1ck](https://github.com/s1ck))
- Add proper Python readme for PyPI [#94](https://github.com/neo4j-labs/graph/pull/94) ([knutwalker](https://github.com/knutwalker))
- Rename `reorder_by_degree` to `make_degree_ordered` [#93](https://github.com/neo4j-labs/graph/pull/93) ([knutwalker](https://github.com/knutwalker))
- Don't repeat the default layout in Python [#92](https://github.com/neo4j-labs/graph/pull/92) ([knutwalker](https://github.com/knutwalker))
- Improve server logging [#91](https://github.com/neo4j-labs/graph/pull/91) ([s1ck](https://github.com/s1ck))
- Add usage demo for Rust [#90](https://github.com/neo4j-labs/graph/pull/90) ([s1ck](https://github.com/s1ck))
- Hook graph_mate into the `cargo release` workflow [#89](https://github.com/neo4j-labs/graph/pull/89) ([knutwalker](https://github.com/knutwalker))
- Update to maturing beta to get the workspace inheritance related fixes [#88](https://github.com/neo4j-labs/graph/pull/88) ([knutwalker](https://github.com/knutwalker))
- Rename unladen_swallow to mate [#87](https://github.com/neo4j-labs/graph/pull/87) ([knutwalker](https://github.com/knutwalker))
- Add optional layout parameter to to_undirected [#86](https://github.com/neo4j-labs/graph/pull/86) ([knutwalker](https://github.com/knutwalker))
- Add to_undirected action to Arrow server [#85](https://github.com/neo4j-labs/graph/pull/85) ([s1ck](https://github.com/s1ck))
- Add demo notebooks [#84](https://github.com/neo4j-labs/graph/pull/84) ([knutwalker](https://github.com/knutwalker))
- Fix public Python API docs/types [#83](https://github.com/neo4j-labs/graph/pull/83) ([knutwalker](https://github.com/knutwalker))
- Add triangle counting to graph_mate [#82](https://github.com/neo4j-labs/graph/pull/82) ([knutwalker](https://github.com/knutwalker))
- Add support for creating graphs from GDL with node values [#79](https://github.com/neo4j-labs/graph/pull/79) ([s1ck](https://github.com/s1ck))

## [graph_mate-v0.0.2](https://github.com/neo4j-labs/graph/tree/graph_mate-v0.0.2) - 2022-11-04

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_mate-v0.0.1...graph_mate-v0.0.2)

## [graph_mate-v0.0.1](https://github.com/neo4j-labs/graph/tree/graph_mate-v0.0.1) - 2022-11-04

[Full Changelog](https://github.com/neo4j-labs/graph/compare/unladen_swallow-v0.2.1...graph_mate-v0.0.1)

### Other

- Switch to stable channel :) [#78](https://github.com/neo4j-labs/graph/pull/78) ([s1ck](https://github.com/s1ck))
- Test fallback implementations [#77](https://github.com/neo4j-labs/graph/pull/77) ([s1ck](https://github.com/s1ck))
- Publish graph-mate to PyPI [#76](https://github.com/neo4j-labs/graph/pull/76) ([knutwalker](https://github.com/knutwalker))
- Improve Page Rank Arrow example [#75](https://github.com/neo4j-labs/graph/pull/75) ([s1ck](https://github.com/s1ck))
- Replace vec_into_raw_parts with safe conversion [#74](https://github.com/neo4j-labs/graph/pull/74) ([s1ck](https://github.com/s1ck))
- Use beta release channel [#73](https://github.com/neo4j-labs/graph/pull/73) ([s1ck](https://github.com/s1ck))
- Use doc_cfg feature only if available [#72](https://github.com/neo4j-labs/graph/pull/72) ([s1ck](https://github.com/s1ck))
- Provide stable fallback for slice_partition_dedup [#71](https://github.com/neo4j-labs/graph/pull/71) ([s1ck](https://github.com/s1ck))
- Remove step_trait feature [#70](https://github.com/neo4j-labs/graph/pull/70) ([knutwalker](https://github.com/knutwalker))
- More inlined variable references into format strings [#69](https://github.com/neo4j-labs/graph/pull/69) ([knutwalker](https://github.com/knutwalker))
- Reduce scope of allow(dead_code) and remove some dead code [#68](https://github.com/neo4j-labs/graph/pull/68) ([knutwalker](https://github.com/knutwalker))
- Remove type_alias_impl_trait feature [#67](https://github.com/neo4j-labs/graph/pull/67) ([knutwalker](https://github.com/knutwalker))
- Provide stable fallback for new_uninit [#66](https://github.com/neo4j-labs/graph/pull/66) ([knutwalker](https://github.com/knutwalker))
- Add RemoveGraph action to Arrow Server [#65](https://github.com/neo4j-labs/graph/pull/65) ([s1ck](https://github.com/s1ck))
- Provide stable fallback for maybe_uninit_write_slice [#64](https://github.com/neo4j-labs/graph/pull/64) ([knutwalker](https://github.com/knutwalker))
- Apply clippy suggestions [#63](https://github.com/neo4j-labs/graph/pull/63) ([knutwalker](https://github.com/knutwalker))
- Add rust-toolchain.toml file [#62](https://github.com/neo4j-labs/graph/pull/62) ([knutwalker](https://github.com/knutwalker))
- Remove unnecessary usize keyword [#61](https://github.com/neo4j-labs/graph/pull/61) ([saguywalker](https://github.com/saguywalker))
- Add action to list graphs on Arrow server [#60](https://github.com/neo4j-labs/graph/pull/60) ([s1ck](https://github.com/s1ck))
- Add lifetime to return type in ToUndirectedEdges [#59](https://github.com/neo4j-labs/graph/pull/59) ([s1ck](https://github.com/s1ck))

## [unladen_swallow-v0.2.1](https://github.com/neo4j-labs/graph/tree/unladen_swallow-v0.2.1) - 2022-10-01

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_app-v0.1.3...unladen_swallow-v0.2.1)

## [graph_app-v0.1.3](https://github.com/neo4j-labs/graph/tree/graph_app-v0.1.3) - 2022-10-01

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_server-v0.1.1...graph_app-v0.1.3)

## [graph_server-v0.1.1](https://github.com/neo4j-labs/graph/tree/graph_server-v0.1.1) - 2022-10-01

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph-v0.2.1...graph_server-v0.1.1)

## [graph-v0.2.1](https://github.com/neo4j-labs/graph/tree/graph-v0.2.1) - 2022-10-01

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_builder-v0.2.1...graph-v0.2.1)

## [graph_builder-v0.2.1](https://github.com/neo4j-labs/graph/tree/graph_builder-v0.2.1) - 2022-10-01

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph-v0.2.0...graph_builder-v0.2.1)

### Other

- Update dependencies [#58](https://github.com/neo4j-labs/graph/pull/58) ([s1ck](https://github.com/s1ck))
- Migrate to clap 4 [#57](https://github.com/neo4j-labs/graph/pull/57) ([s1ck](https://github.com/s1ck))
- Use new workspace dependency inheritance feature [#56](https://github.com/neo4j-labs/graph/pull/56) ([s1ck](https://github.com/s1ck))
- Run tests with cargo careful [#55](https://github.com/neo4j-labs/graph/pull/55) ([s1ck](https://github.com/s1ck))
- GATs are stable [#54](https://github.com/neo4j-labs/graph/pull/54) ([s1ck](https://github.com/s1ck))
- Upgrade dependencies [#52](https://github.com/neo4j-labs/graph/pull/52) ([s1ck](https://github.com/s1ck))
- Replace usage of rayon::scope with std::thread::scope [#51](https://github.com/neo4j-labs/graph/pull/51) ([s1ck](https://github.com/s1ck))
- Link to libpython by default [#50](https://github.com/neo4j-labs/graph/pull/50) ([knutwalker](https://github.com/knutwalker))
- Expose Wcc in unladen_swallow [#49](https://github.com/neo4j-labs/graph/pull/49) ([s1ck](https://github.com/s1ck))
- Use generic graph args in algos [#47](https://github.com/neo4j-labs/graph/pull/47) ([s1ck](https://github.com/s1ck))
- Support windows style line breaks [#45](https://github.com/neo4j-labs/graph/pull/45) ([s1ck](https://github.com/s1ck))
- Replace usize with u64 in apps [#44](https://github.com/neo4j-labs/graph/pull/44) ([s1ck](https://github.com/s1ck))
- Move [warmup-]run logic into dedicated function [#43](https://github.com/neo4j-labs/graph/pull/43) ([s1ck](https://github.com/s1ck))
- Add edge-list like methods to Python extension [#42](https://github.com/neo4j-labs/graph/pull/42) ([knutwalker](https://github.com/knutwalker))
- Proposal for iterator based neighbor access [#39](https://github.com/neo4j-labs/graph/pull/39) ([s1ck](https://github.com/s1ck))
- Change default csr layout to Unsorted [#38](https://github.com/neo4j-labs/graph/pull/38) ([s1ck](https://github.com/s1ck))
- Add Python module extension [#32](https://github.com/neo4j-labs/graph/pull/32) ([knutwalker](https://github.com/knutwalker))

## [graph-v0.2.0](https://github.com/neo4j-labs/graph/tree/graph-v0.2.0) - 2022-03-13

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_builder-v0.2.0...graph-v0.2.0)

## [graph_builder-v0.2.0](https://github.com/neo4j-labs/graph/tree/graph_builder-v0.2.0) - 2022-03-13

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph-v0.1.15...graph_builder-v0.2.0)

### Other

- Add to_undirected ops for directed graphs [#37](https://github.com/neo4j-labs/graph/pull/37) ([knutwalker](https://github.com/knutwalker))
- Support wcc in arrow server [#36](https://github.com/neo4j-labs/graph/pull/36) ([s1ck](https://github.com/s1ck))
- Replace AtomicIdx with atomic crate [#35](https://github.com/neo4j-labs/graph/pull/35) ([s1ck](https://github.com/s1ck))
- Merge algo apps into single app [#34](https://github.com/neo4j-labs/graph/pull/34) ([s1ck](https://github.com/s1ck))
- Pass max_node_id to edge list in graph500 [#33](https://github.com/neo4j-labs/graph/pull/33) ([s1ck](https://github.com/s1ck))
- Parallelize Graph500 Input  [#31](https://github.com/neo4j-labs/graph/pull/31) ([s1ck](https://github.com/s1ck))
- Remove vec_into_raw_parts feature [#30](https://github.com/neo4j-labs/graph/pull/30) ([s1ck](https://github.com/s1ck))
- Weakly Connected Components [#29](https://github.com/neo4j-labs/graph/pull/29) ([s1ck](https://github.com/s1ck))
- Algorithm configs [#28](https://github.com/neo4j-labs/graph/pull/28) ([s1ck](https://github.com/s1ck))
- Arrow Server PoC [#27](https://github.com/neo4j-labs/graph/pull/27) ([s1ck](https://github.com/s1ck))

## [graph-v0.1.15](https://github.com/neo4j-labs/graph/tree/graph-v0.1.15) - 2022-01-16

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph-v0.1.14...graph-v0.1.15)

## [graph-v0.1.14](https://github.com/neo4j-labs/graph/tree/graph-v0.1.14) - 2022-01-14

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph-v0.1.13...graph-v0.1.14)

## [graph-v0.1.13](https://github.com/neo4j-labs/graph/tree/graph-v0.1.13) - 2022-01-14

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_app-v0.1.2...graph-v0.1.13)

## [graph_app-v0.1.2](https://github.com/neo4j-labs/graph/tree/graph_app-v0.1.2) - 2022-01-14

[Full Changelog](https://github.com/neo4j-labs/graph/compare/graph_builder-v0.1.13...graph_app-v0.1.2)

## [graph_builder-v0.1.13](https://github.com/neo4j-labs/graph/tree/graph_builder-v0.1.13) - 2022-01-14

[Full Changelog](https://github.com/neo4j-labs/graph/compare/4b0c3c0167d46871968bc1e307afe0578a8cc83a...graph_builder-v0.1.13)
