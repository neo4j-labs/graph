macro_rules! gen_runner {
    (directed+unweighted: $algo_name:ident, $algo_func:expr, $algo_config:ty) => {
        mod $algo_name {
            use graph::prelude::*;
            crate::gen_runner!(__entry: $algo_config);
            crate::gen_runner!(__run_file_format_all: $algo_config, ());
            crate::gen_runner!(__run_graph_format: $algo_config, ());
            crate::gen_runner!(__bench: $algo_func, $algo_config, DirectedNeighbors<NI>, ());
        }
    };

    (directed+weighted: $algo_name:ident, $algo_func:expr, $algo_config:ty, $ev_type:ty) => {
        mod $algo_name {
            use graph::prelude::*;
            crate::gen_runner!(__entry: $algo_config);
            crate::gen_runner!(__run_file_format_edge_list: $algo_config, $ev_type);
            crate::gen_runner!(__run_graph_format: $algo_config, $ev_type);
            crate::gen_runner!(__bench: $algo_func, $algo_config, DirectedNeighborsWithValues<NI, $ev_type>, $ev_type);
        }
    };

    (__entry: $algo_config:ty) => {
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
    };

    (__run_file_format_all: $algo_config:ty, $ev_type:ty) => {
        fn run_<NI: Idx>(
            args: $crate::CommonArgs,
            config: $algo_config,
        ) -> ::kommandozeile::Result<()>
        where
            NI: Idx + ::std::hash::Hash,
        {
            match args.format {
                $crate::FileFormat::EdgeList => {
                    run__::<NI, EdgeListInput<NI, $ev_type>>(args, config, EdgeListInput::default())
                }
                $crate::FileFormat::Graph500 => {
                    run__::<NI, Graph500Input<NI>>(args, config, Graph500Input::default())
                }
            }
        }
    };

    (__run_file_format_edge_list: $algo_config:ty, $ev_type:ty) => {
        fn run_<NI: Idx>(
            args: $crate::CommonArgs,
            config: $algo_config,
        ) -> ::kommandozeile::Result<()>
        where
            NI: Idx + ::std::hash::Hash,
        {
            match args.format {
                $crate::FileFormat::EdgeList => {
                    run__::<NI, EdgeListInput<NI, $ev_type>>(args, config, EdgeListInput::default())
                }
                $crate::FileFormat::Graph500 => {
                    std::panic!("Graph500 is not supported for weighted graphs")
                }
            }
        }
    };

    (__run_graph_format: $algo_config:ty, $ev_type:ty) => {
        fn run__<NI, Format>(
            args: $crate::CommonArgs,
            config: $algo_config,
            file_format: Format,
        ) -> ::kommandozeile::Result<()>
        where
            NI: Idx + ::std::hash::Hash,
            Format: InputCapabilities<NI>,
            Format::GraphInput: TryFrom<InputPath<::std::path::PathBuf>>,
            <Format as InputCapabilities<NI>>::GraphInput: Edges<NI = NI, EV = $ev_type>,
            Error:
                From<<Format::GraphInput as TryFrom<InputPath<::std::path::PathBuf>>>::Error>,
        {
            match args.graph {
                $crate::GraphFormat::CompressedSparseRow => {
                    run___::<DirectedCsrGraph<NI, (), $ev_type>, NI, Format>(args, config, file_format)
                }
                $crate::GraphFormat::AdjacencyList => {
                    run___::<DirectedALGraph<NI, (), $ev_type>, NI, Format>(args, config, file_format)
                }
            }
        }
    };

    (__bench: $algo_func:expr, $algo_config:ty, $neighbors_trait:path, $ev_type:ty) => {
        fn run___<G, NI, Format>(
            args: $crate::CommonArgs,
            config: $algo_config,
            file_format: Format,
        ) -> ::kommandozeile::Result<()>
        where
            NI: Idx + ::std::hash::Hash,
            Format: InputCapabilities<NI>,
            Format::GraphInput: TryFrom<InputPath<::std::path::PathBuf>>,
            <Format as InputCapabilities<NI>>::GraphInput: Edges<NI = NI, EV = $ev_type>,
            Error:
                From<<Format::GraphInput as TryFrom<InputPath<::std::path::PathBuf>>>::Error>,
            G: Graph<NI> + DirectedDegrees<NI> + $neighbors_trait + Sync,
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
    };
}

pub(crate) use gen_runner;
