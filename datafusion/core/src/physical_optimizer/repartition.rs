// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Repartition optimizer that introduces repartition nodes to increase the level of parallelism available
use std::sync::Arc;

use super::optimizer::PhysicalOptimizerRule;
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_plan::Partitioning::*;
use crate::physical_plan::{
    repartition::RepartitionExec, with_new_children_if_necessary, ExecutionPlan,
};

/// Optimizer that introduces repartition to introduce more
/// parallelism in the plan
///
/// For example, given an input such as:
///
///
/// ```text
/// ┌─────────────────────────────────┐
/// │                                 │
/// │          ExecutionPlan          │
/// │                                 │
/// └─────────────────────────────────┘
///             ▲         ▲
///             │         │
///       ┌─────┘         └─────┐
///       │                     │
///       │                     │
///       │                     │
/// ┌───────────┐         ┌───────────┐
/// │           │         │           │
/// │ batch A1  │         │ batch B1  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A2  │         │ batch B2  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A3  │         │ batch B3  │
/// │           │         │           │
/// └───────────┘         └───────────┘
///
///      Input                 Input
///        A                     B
/// ```
///
/// This optimizer will attempt to add a `RepartitionExec` to increase
/// the parallelism (to 3 in this case)
///
/// ```text
///     ┌─────────────────────────────────┐
///     │                                 │
///     │          ExecutionPlan          │
///     │                                 │
///     └─────────────────────────────────┘
///               ▲      ▲       ▲            Input now has 3
///               │      │       │             partitions
///       ┌───────┘      │       └───────┐
///       │              │               │
///       │              │               │
/// ┌───────────┐  ┌───────────┐   ┌───────────┐
/// │           │  │           │   │           │
/// │ batch A1  │  │ batch A3  │   │ batch B3  │
/// │           │  │           │   │           │
/// ├───────────┤  ├───────────┤   ├───────────┤
/// │           │  │           │   │           │
/// │ batch B2  │  │ batch B1  │   │ batch A2  │
/// │           │  │           │   │           │
/// └───────────┘  └───────────┘   └───────────┘
///       ▲              ▲               ▲
///       │              │               │
///       └─────────┐    │    ┌──────────┘
///                 │    │    │
///                 │    │    │
///     ┌─────────────────────────────────┐   batches are
///     │       RepartitionExec(3)        │   repartitioned
///     │           RoundRobin            │
///     │                                 │
///     └─────────────────────────────────┘
///                 ▲         ▲
///                 │         │
///           ┌─────┘         └─────┐
///           │                     │
///           │                     │
///           │                     │
///     ┌───────────┐         ┌───────────┐
///     │           │         │           │
///     │ batch A1  │         │ batch B1  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A2  │         │ batch B2  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A3  │         │ batch B3  │
///     │           │         │           │
///     └───────────┘         └───────────┘
///
///
///      Input                 Input
///        A                     B
/// ```
#[derive(Default)]
pub struct Repartition {}

impl Repartition {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Recursively attempts to increase the overall parallelism of the
/// plan, while respecting ordering, by adding a `RepartitionExec` at
/// the output of `plan` if it would help parallelism and not destroy
/// any possibly useful ordering.
///
/// It does so using a depth first scan of the tree, and repartitions
/// any plan that:
///
/// 1. Has fewer partitions than `target_partitions`
///
/// 2. Has a direct parent that `benefits_from_input_partitioning`
///
/// 3. Does not destroy any existing sort order if the parent is
/// relying on it.
///
/// if `can_reorder` is false, it means the parent node of `plan` is
/// trying to take advantage of the output sort order of plan, so it
/// should not be repartitioned if doing so would destroy the output
/// sort order.
///
/// (Parent)   - If can_reorder is false, means this parent node is
///              trying to use the sort ouder order this plan. If true
///              means parent doesn't care about sort order
///
/// (plan)     - We are deciding to add a partition above here
///
/// (children) - Recursively visit all children first
///
/// If 'would_benefit` is true, the upstream operator would benefit
/// from additional partitions and thus repatitioning is considered.
///
/// if `is_root` is true, no repartition is added.
fn optimize_partitions(
    target_partitions: usize,
    plan: Arc<dyn ExecutionPlan>,
    is_root: bool,
    can_reorder: bool,
    would_benefit: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Recurse into children bottom-up (attempt to repartition as
    // early as possible)
    let new_plan = if plan.children().is_empty() {
        // leaf node - don't replace children
        plan
    } else {
        let children = plan
            .children()
            .iter()
            .map(|child| {
                // does plan itelf (not parent) require its input to
                // be sorted in some way?
                let required_input_ordering =
                    plan_has_required_input_ordering(plan.as_ref());

                let can_reorder_child = if can_reorder {
                    // parent of `plan` will not use any particular order

                    // if `plan` itself doesn't need order OR
                    !required_input_ordering ||
                    // child has no order to preserve
                        child.output_ordering().is_none()
                } else {
                    // parent would like to use the `plan`'s output
                    // order.

                    // if `plan` doesn't maintain the input order and
                    // doesn't need the child's output order itself
                    (!plan.maintains_input_order() &&  !required_input_ordering) ||
                    // child has no ordering to preserve
                        child.output_ordering().is_none()
                };

                optimize_partitions(
                    target_partitions,
                    child.clone(),
                    false, // child is not root
                    can_reorder_child,
                    plan.benefits_from_input_partitioning(),
                )
            })
            .collect::<Result<_>>()?;
        with_new_children_if_necessary(plan, children)?
    };

    // decide if we should bother trying to repartition the output of this plan
    let mut could_repartition = match new_plan.output_partitioning() {
        // Apply when underlying node has less than `self.target_partitions` amount of concurrency
        RoundRobinBatch(x) => x < target_partitions,
        UnknownPartitioning(x) => x < target_partitions,
        // we don't want to introduce partitioning after hash partitioning
        // as the plan will likely depend on this
        Hash(_, _) => false,
    };

    // Don't need to apply when the returned row count is not greater than 1
    let stats = new_plan.statistics();
    if stats.is_exact {
        could_repartition = could_repartition
            && stats.num_rows.map(|num_rows| num_rows > 1).unwrap_or(true);
    }

    // don't reparititon root of the plan
    if is_root {
        could_repartition = false;
    }

    if would_benefit && could_repartition && can_reorder {
        Ok(Arc::new(RepartitionExec::try_new(
            new_plan,
            RoundRobinBatch(target_partitions),
        )?))
    } else {
        Ok(new_plan)
    }
}

/// Returns true if `plan` requires any of inputs to be sorted in some
/// way for correctness. If this is true, its output should not be
/// repartitioned if it would destroy the required order.
fn plan_has_required_input_ordering(plan: &dyn ExecutionPlan) -> bool {
    // NB: checking `is_empty()` is not the right check!
    plan.required_input_ordering().iter().any(Option::is_some)
}

impl PhysicalOptimizerRule for Repartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = config.execution.target_partitions;
        let enabled = config.optimizer.enable_round_robin_repartition;
        // Don't run optimizer if target_partitions == 1
        if !enabled || target_partitions == 1 {
            Ok(plan)
        } else {
            let is_root = true;
            let can_reorder = plan.output_ordering().is_none();
            let would_benefit = false;
            optimize_partitions(
                target_partitions,
                plan.clone(),
                is_root,
                can_reorder,
                would_benefit,
            )
        }
    }

    fn name(&self) -> &str {
        "repartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_optimizer::sort_enforcement::EnforceSorting;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::{col, PhysicalSortExpr};
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::{displayable, DisplayFormatType, Statistics};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c1", DataType::Boolean, true)]))
    }

    /// Create a non sorted parquet exec
    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    // Created a sorted parquet exec
    fn parquet_exec_sorted() -> Arc<ParquetExec> {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];

        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(sort_exprs),
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    fn sort_preserving_merge_exec(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let expr = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: arrow::compute::SortOptions::default(),
        }];

        Arc::new(SortPreservingMergeExec::new(expr, input))
    }

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(col("c1", &schema()).unwrap(), input).unwrap())
    }

    fn sort_exec(
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];
        Arc::new(SortExec::new_with_partitioning(
            sort_exprs,
            input,
            preserve_partitioning,
            None,
        ))
    }

    fn projection_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let exprs = vec![(col("c1", &schema()).unwrap(), "c1".to_string())];
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                vec![],
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        PhysicalGroupBy::default(),
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            0,
            Some(100),
        ))
    }

    fn limit_exec_with_skip(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            5,
            Some(100),
        ))
    }

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    fn sort_required_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortRequiredExec::new(input))
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            let mut config = ConfigOptions::new();
            config.execution.target_partitions = 10;

            // run optimizer
            let optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
                Arc::new(Repartition::new()),
                // EnforceDistribution is an essential rule to be applied.
                // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                Arc::new(EnforceDistribution::new()),
                // EnforceSorting is an essential rule to be applied.
                // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                Arc::new(EnforceSorting::new()),
            ];
            let optimized = optimizers.into_iter().fold($PLAN, |plan, optimizer| {
                optimizer.optimize(plan, &config).unwrap()
            });

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent().to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let plan = aggregate(parquet_exec());

        let expected = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let plan = aggregate(filter_exec(parquet_exec()));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit() -> Result<()> {
        let plan = limit_exec(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit_with_skip() -> Result<()> {
        let plan = limit_exec_with_skip(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=5, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit() -> Result<()> {
        let plan = limit_exec(sort_exec(parquet_exec(), false));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: [c1@0 ASC]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit_with_filter() -> Result<()> {
        let plan = limit_exec(filter_exec(sort_exec(parquet_exec(), false)));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // data is sorted so can't repartition here even though
            // filter would benefit from parallelism, the answers might be wrong
            "SortExec: [c1@0 ASC]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit() -> Result<()> {
        let plan = aggregate(limit_exec(filter_exec(limit_exec(parquet_exec()))));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit_with_skip() -> Result<()> {
        let plan = aggregate(limit_exec_with_skip(filter_exec(limit_exec(
            parquet_exec(),
        ))));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=5, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    // repartition works differently for limit when there is a sort below it

    #[test]
    fn repartition_ignores_union() -> Result<()> {
        let plan = union_exec(vec![parquet_exec(); 5]);

        let expected = &[
            "UnionExec",
            // Expect no repartition of ParquetExec
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_through_sort_preserving_merge() -> Result<()> {
        // sort preserving merge with non-sorted input
        let plan = sort_preserving_merge_exec(parquet_exec());

        // need repartiton and resort as the data was not sorted correctly
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "SortExec: [c1@0 ASC]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge() -> Result<()> {
        // sort preserving merge already sorted input,
        let plan = sort_preserving_merge_exec(parquet_exec_sorted());

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[c1@0 ASC], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge_with_union() -> Result<()> {
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let input = union_exec(vec![parquet_exec_sorted(); 2]);
        let plan = sort_preserving_merge_exec(input);

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[c1@0 ASC], projection=[c1]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[c1@0 ASC], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort() -> Result<()> {
        //  SortRequired
        //    Parquet(sorted)

        let plan = sort_required_exec(parquet_exec_sorted());

        // should not repartition as doing so destroys the necessary sort order
        let expected = &[
            "SortRequiredExec",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[c1@0 ASC], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort_more_complex() -> Result<()> {
        // model a more complicated scenario where one child of a union can be repartitioned for performance
        // but the other can not be
        //
        // Union
        //  SortRequired
        //    Parquet(sorted)
        //  Filter
        //    Parquet(unsorted)

        let input1 = sort_required_exec(parquet_exec_sorted());
        let input2 = filter_exec(parquet_exec());
        let plan = union_exec(vec![input1, input2]);

        // should not repartition below the SortRequired as that
        // destroys the sort order but should still repartition for
        // FilterExec
        let expected = &[
            "UnionExec",
            // union input 1: no repartitioning
            "SortRequiredExec",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[c1@0 ASC], projection=[c1]",
            // union input 2: should repartition
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_with_projection() -> Result<()> {
        // non sorted input
        let plan = sort_preserving_merge_exec(projection_exec(parquet_exec()));

        // needs to repartition / sort as the data was not sorted correctly
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "SortExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_transitively_with_projection() -> Result<()> {
        // sorted input
        let plan = sort_preserving_merge_exec(projection_exec(parquet_exec_sorted()));

        // data should not be repartitioned / resorted
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[c1@0 ASC], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection() -> Result<()> {
        let plan =
            sort_preserving_merge_exec(sort_exec(projection_exec(parquet_exec()), true));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_filter() -> Result<()> {
        let plan =
            sort_preserving_merge_exec(sort_exec(filter_exec(parquet_exec()), true));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: [c1@0 ASC]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
        let plan = sort_preserving_merge_exec(sort_exec(
            projection_exec(filter_exec(parquet_exec())),
            true,
        ));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "FilterExec: c1@0",
            // repartition is lowest down
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    /// Models operators like BoundedWindowExec that require an input
    /// ordering but is easy to construct
    #[derive(Debug)]
    struct SortRequiredExec {
        input: Arc<dyn ExecutionPlan>,
    }

    impl SortRequiredExec {
        fn new(input: Arc<dyn ExecutionPlan>) -> Self {
            Self { input }
        }
    }

    impl ExecutionPlan for SortRequiredExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }

        fn output_partitioning(&self) -> crate::physical_plan::Partitioning {
            self.input.output_partitioning()
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            self.input.output_ordering()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.input.clone()]
        }

        // model that it requires the output ordering of its input
        fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
            vec![self.input.output_ordering()]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq!(children.len(), 1);
            let child = children.pop().unwrap();
            Ok(Arc::new(Self::new(child)))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<crate::execution::context::TaskContext>,
        ) -> Result<crate::physical_plan::SendableRecordBatchStream> {
            unreachable!();
        }

        fn statistics(&self) -> Statistics {
            self.input.statistics()
        }

        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "SortRequiredExec")
        }
    }
}
