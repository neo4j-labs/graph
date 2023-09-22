macro_rules! runner {
    ($algo_name:ident, $algo_func:expr, $algo_config:ty) => {
        mod $algo_name {
            use graph::prelude::*;

            pub(crate) fn run(
                args: $crate::CommonArgs,
                config: $algo_config,
            ) -> ::kommandozeile::Result<()> {
                ::log::info!(
                    "Reading graph ({} bit) from: {:?}",
                    if args.use_32_bit { "32" } else { "64" },
                    args.path
                );

                if args.use_32_bit {
                    run_::<u32>(args, config)
                } else {
                    run_::<u64>(args, config)
                }
            }

            fn run_<NI: Idx>(
                args: $crate::CommonArgs,
                config: PageRankConfig,
            ) -> ::kommandozeile::Result<()> {
                match args.format {
                    $crate::FileFormat::EdgeList => {
                        run__::<NI, EdgeListInput<NI>>(args, config, EdgeListInput::default())
                    }
                    $crate::FileFormat::Graph500 => {
                        run__::<NI, Graph500Input<NI>>(args, config, Graph500Input::default())
                    }
                }
            }

            fn run__<NI, Format>(
                args: $crate::CommonArgs,
                config: PageRankConfig,
                file_format: Format,
            ) -> ::kommandozeile::Result<()>
            where
                NI: Idx,
                Format: InputCapabilities<NI>,
                Format::GraphInput: TryFrom<InputPath<::std::path::PathBuf>>,
                <Format as InputCapabilities<NI>>::GraphInput: Edges<NI = NI, EV = ()>,
                Error:
                    From<<Format::GraphInput as TryFrom<InputPath<::std::path::PathBuf>>>::Error>,
            {
                match args.graph {
                    $crate::GraphFormat::CompressedSparseRow => {
                        run___::<DirectedCsrGraph<NI>, NI, Format>(args, config, file_format)
                    }
                    $crate::GraphFormat::AdjacencyList => {
                        run___::<DirectedALGraph<NI>, NI, Format>(args, config, file_format)
                    }
                }
            }

            fn run___<G, NI, Format>(
                args: $crate::CommonArgs,
                config: PageRankConfig,
                file_format: Format,
            ) -> ::kommandozeile::Result<()>
            where
                NI: Idx,
                Format: InputCapabilities<NI>,
                Format::GraphInput: TryFrom<InputPath<::std::path::PathBuf>>,
                <Format as InputCapabilities<NI>>::GraphInput: Edges<NI = NI, EV = ()>,
                Error:
                    From<<Format::GraphInput as TryFrom<InputPath<::std::path::PathBuf>>>::Error>,
                G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
                G: TryFrom<(Format::GraphInput, CsrLayout)>,
                Error: From<<G as TryFrom<(Format::GraphInput, CsrLayout)>>::Error>,
            {
                let graph: G = GraphBuilder::new()
                    .csr_layout(CsrLayout::Sorted)
                    .file_format(file_format)
                    .path(args.path)
                    .build()?;

                $crate::time(args.runs, args.warmup_runs, || {
                    $algo_func(&graph, config);
                });

                Ok(())
            }
        }
    };
}

pub(crate) use runner;
