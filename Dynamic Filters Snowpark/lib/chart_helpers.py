"""
Copyright (c) 2022 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from typing import Iterable, List, Tuple

from snowflake.snowpark.table import Table


def mk_labels(_iter: Iterable[str]) -> Tuple[str, ...]:
    """Produces the labels configuration for plotly.graph_objects.Sankey"""
    first_label = "Original data"
    return (first_label,) + tuple(map(lambda _: f"Filter: '{_}'", _iter)) + ("Result",)


def mk_links(table_sequence: List[Table]) -> dict:
    """Produces the links configuration for plotly.graph_objects.Sankey"""
    return dict(
        source=list(range(len(table_sequence))),
        target=list(range(1, len(table_sequence) + 1)),
        value=[_.count() for _ in table_sequence],
    )
